package net.asksakis.massdroidv2.tv.ui

import android.util.Log
import net.asksakis.massdroidv2.domain.player.VolumeKeyController
import net.asksakis.massdroidv2.domain.player.VolumeKeyTarget
import net.asksakis.massdroidv2.domain.player.volumeKeyTarget
import kotlinx.coroutines.flow.combine
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import net.asksakis.massdroidv2.data.sendspin.SendspinManager
import net.asksakis.massdroidv2.data.sendspin.SyncState
import net.asksakis.massdroidv2.data.websocket.ConnectionState
import net.asksakis.massdroidv2.data.websocket.MaWebSocketClient
import net.asksakis.massdroidv2.data.websocket.MaCommands
import net.asksakis.massdroidv2.data.websocket.TransferQueueArgs
import net.asksakis.massdroidv2.domain.model.PlaybackState
import net.asksakis.massdroidv2.domain.model.Player
import net.asksakis.massdroidv2.domain.model.PlayerType
import net.asksakis.massdroidv2.playback.OfflinePlayPausePolicy
import net.asksakis.massdroidv2.domain.model.skipAvailability
import net.asksakis.massdroidv2.domain.player.QueueTransfer
import net.asksakis.massdroidv2.domain.player.userMessage
import net.asksakis.massdroidv2.domain.repository.MusicRepository
import net.asksakis.massdroidv2.domain.repository.PlayerRepository
import net.asksakis.massdroidv2.domain.repository.SettingsRepository
import javax.inject.Inject

private const val TAG = "TvMiniPlayerVM"

/**
 * Backs the mini player bar: the selected player's live state, the full player list
 * for the switch dialog, and the transport buttons on the bar.
 */
@HiltViewModel
class TvMiniPlayerViewModel @Inject constructor(
    private val playerRepository: PlayerRepository,
    private val settingsRepository: SettingsRepository,
    musicRepository: MusicRepository,
    private val volumeKeyController: VolumeKeyController,
    private val wsClient: MaWebSocketClient,
    sendspinManager: SendspinManager,
) : ViewModel() {

    val players: StateFlow<List<Player>> = playerRepository.players
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    val selectedPlayer: StateFlow<Player?> = playerRepository.selectedPlayer

    /** MA player id of this device's own Sendspin player (the local "MassDroid TV"). */
    val localPlayerId: StateFlow<String?> = settingsRepository.sendspinClientId
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), null)

    private val queueTransfer = QueueTransfer(musicRepository, playerRepository)
    val queueState = playerRepository.queueState

    val transferableQueueId: StateFlow<String?> = playerRepository.queueState
        .map { queue -> queue?.takeIf { it.active && it.currentItem != null }?.queueId }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), null)

    suspend fun defaultToLocalPlayer(localId: String): Player? {
        val previousId = settingsRepository.selectedPlayerId.first()
        val previous = players.value.firstOrNull {
            it.playerId == previousId && it.playerId != localId && it.available && !it.hideInUi
        }
        selectPlayer(localId)
        return previous
    }

    fun selectPlayer(playerId: String) = playerRepository.selectPlayer(playerId)

    /** The server advertises which individual outputs can join the active queue owner. */
    fun syncMembersToAdd(players: List<Player>, sourceId: String, targetId: String): List<String>? {
        val byId = players.associateBy { it.playerId }
        val source = byId[sourceId] ?: return null
        val target = byId[targetId] ?: return null
        if (!source.available || !target.available || "set_members" !in source.supportedFeatures ||
            source.syncedTo != null || source.provider.startsWith("universal_group") ||
            sourceId.startsWith("ugp_")
        ) return null

        // A group preset contributes its speakers to the source's sync session.
        // Sending the preset itself as a child would try to nest the group.
        val requested = outputMembers(target)
        if (requested.isEmpty()) return null
        val existing = source.groupChilds.toSet() + sourceId
        val additions = requested.filterNot { it in existing }
        return additions.takeIf { ids ->
            ids.all { id ->
                val child = byId[id]
                child != null && child.available && child.type != PlayerType.GROUP &&
                    id in source.canGroupWith &&
                    (child.activeGroup == null || child.activeGroup == sourceId) &&
                    (child.syncedTo == null || child.syncedTo == sourceId) &&
                    child.groupChilds.none { it != id }
            }
        }
    }

    suspend fun addSyncedPlayback(sourceId: String, targetId: String): String {
        val queue = playerRepository.queueState.value
        if (queue == null || queue.queueId != sourceId || !queue.active || queue.currentItem == null) {
            return "The current queue changed. Open Speakers and try again."
        }
        val currentPlayers = playerRepository.players.value
        val additions = syncMembersToAdd(currentPlayers, sourceId, targetId)
            ?: return "These speakers can't sync with the current output."
        if (additions.isEmpty()) return "These speakers are already part of this playback."
        val targetName = currentPlayers.first { it.playerId == targetId }.displayName
        try {
            playerRepository.setGroupMembers(sourceId, addIds = additions, removeIds = null)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            Log.w(TAG, "Couldn't join speakers to the active queue", error)
            return "Couldn't confirm that $targetName joined. Check the speaker list."
        }
        val confirmed = withTimeoutOrNull(5_000) {
            playerRepository.players.first { updated ->
                val members = updated.firstOrNull { it.playerId == sourceId }?.groupChilds.orEmpty()
                additions.all { it in members }
            }
        }
        return if (confirmed != null) "Added $targetName to current playback"
        else "Couldn't confirm that $targetName joined. Check the speaker list."
    }

    private fun outputMembers(player: Player): Set<String> {
        val children = (player.staticGroupMembers + player.groupChilds).toSet()
        return if (player.type == PlayerType.GROUP) children else children + player.playerId
    }

    fun restartGroupMembers(players: List<Player>, sourceId: String, targetId: String): Set<String>? {
        val byId = players.associateBy { it.playerId }
        val source = byId[sourceId]?.takeIf { it.available } ?: return null
        val target = byId[targetId]?.takeIf { it.available } ?: return null
        val existing = outputMembers(source)
        val requested = outputMembers(target)
        if (existing.isEmpty() || requested.isEmpty() || existing.containsAll(requested)) return null
        return (existing + requested).takeIf { members ->
            members.all { id ->
                val member = byId[id]
                member != null && member.available && member.type != PlayerType.GROUP &&
                    (member.activeGroup == null || member.activeGroup == sourceId) &&
                    (member.syncedTo == null || member.syncedTo == sourceId || member.syncedTo in existing)
            }
        }
    }

    suspend fun addAndRestart(sourceId: String, targetId: String, trackId: String): String {
        val queue = queueState.value
        if (queue?.queueId != sourceId || queue.currentItem?.queueItemId != trackId) {
            return "The current track changed. Open Speakers and try again."
        }
        val current = playerRepository.players.value
        val members = restartGroupMembers(current, sourceId, targetId)
            ?: return "These outputs can't share a playback group right now."
        val source = current.first { it.playerId == sourceId }
        val target = current.first { it.playerId == targetId }
        var destinationId: String? = null
        var queueMoved = false
        try {
            val existingGroup = current.firstOrNull {
                it.available && it.type == PlayerType.GROUP && it.playerId != sourceId &&
                    it.state == PlaybackState.IDLE && outputMembers(it) == members
            }
            val destination = existingGroup ?: run {
                var provider = playerRepository.getGroupCapableProviders().firstOrNull {
                    it.domain == "universal_group"
                }
                if (provider == null) {
                    wsClient.sendCommand("config/providers/save", buildJsonObject {
                        put("provider_domain", "universal_group")
                        put("values", buildJsonObject {})
                    }, retryAfterTimeout = false)
                    provider = playerRepository.getGroupCapableProviders().firstOrNull {
                        it.domain == "universal_group"
                    }
                }
                val providerId = provider?.instanceId ?: return "Music Assistant couldn't enable shared playback."
                val name = "${source.displayName} + ${target.displayName}"
                val before = playerRepository.players.value.map { it.playerId }.toSet()
                playerRepository.createGroupPlayer(providerId, name, members.toList())
                withTimeoutOrNull(8_000) {
                    playerRepository.players.first { updated ->
                        updated.any { it.playerId !in before && it.provider == providerId && outputMembers(it) == members }
                    }.first { it.playerId !in before && it.provider == providerId && outputMembers(it) == members }
                } ?: return "Music Assistant didn't confirm the new playback group."
            }
            destinationId = destination.playerId
            val latest = queueState.value
            if (latest?.queueId != sourceId || latest.currentItem?.queueItemId != trackId) {
                return "The current track changed. Open Speakers and try again."
            }
            wsClient.sendCommand(
                MaCommands.PlayerQueues.TRANSFER,
                TransferQueueArgs(sourceQueueId = sourceId, targetQueueId = destinationId, autoPlay = false).toJson(),
                retryAfterTimeout = false,
            )
            queueMoved = true
            playerRepository.selectPlayer(destinationId)
            wsClient.sendCommand(MaCommands.PlayerQueues.PLAY_INDEX, buildJsonObject {
                put("queue_id", destinationId)
                put("index", trackId)
                put("seek_position", 0)
            }, retryAfterTimeout = false)
            return "Restarted the current track on ${destination.displayName}"
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            Log.w(TAG, "Couldn't restart playback on the shared group", error)
            return if (queueMoved) "Queue moved to the group. Press Play if it hasn't started."
            else if (destinationId != null) "Couldn't confirm the queue move. Check the source and group."
            else "Couldn't prepare shared playback. Check the speakers and Music Assistant permissions."
        }
    }

    suspend fun moveQueue(sourceId: String, targetId: String): String {
        val queue = playerRepository.queueState.value
        if (queue == null || queue.queueId != sourceId || !queue.active || queue.currentItem == null) {
            return "The current queue changed. Open Speakers and try again."
        }
        if (players.value.none { it.playerId == targetId && it.available }) {
            return "That speaker is unavailable."
        }
        return queueTransfer.moveAndFollow(sourceId, targetId).userMessage()
    }

    /**
     * The play/pause button. With MA down and this TV's own player selected it follows the
     * local output, which the last state MA reported does not; see [OfflinePlayPausePolicy].
     * Eagerly, because playPause() reads it for the direction of the press.
     */
    val playPauseButton: StateFlow<OfflinePlayPausePolicy.Button> = combine(
        wsClient.connectionState,
        selectedPlayer,
        localPlayerId,
        sendspinManager.streamActive,
        sendspinManager.syncState,
    ) { connection, player, localId, streamActive, syncState ->
        OfflinePlayPausePolicy.button(
            maConnected = connection is ConnectionState.Connected,
            serverPlaying = player?.state == PlaybackState.PLAYING,
            localPlayerId = localId,
            selectedPlayerId = player?.playerId,
            streamActive = streamActive,
            outputIdle = syncState == SyncState.IDLE,
        )
    }.stateIn(viewModelScope, SharingStarted.Eagerly, OfflinePlayPausePolicy.Button(false, false))

    /**
     * Whether the volume buttons show: only for a Music Assistant player elsewhere, decided as
     * the remote's volume keys are ([volumeKeyTarget]). This TV's own output, a member of a
     * group included, is left at full level for the TV or the receiver to set: turning it down
     * here as well would attenuate twice. The keys rarely reach the app on a TV (a remote with
     * IR or CEC control sends them straight to the receiver), so these buttons are the way.
     */
    val volumeShown: StateFlow<Boolean> = combine(selectedPlayer, localPlayerId) { player, localId ->
        volumeKeyTarget(player, localId) is VolumeKeyTarget.RemotePlayer
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), false)

    val canPrevious: StateFlow<Boolean> = playerRepository.queueState
        .map { it.skipAvailability().previous }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), true)

    /** False when next would do nothing, for example on a radio alone in the queue. */
    val canNext: StateFlow<Boolean> = playerRepository.queueState
        .map { it.skipAvailability().next }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), true)

    /** A step on the selected player, through the same controller as the volume keys. */
    fun volumeUp() {
        volumeKeyController.step(up = true)
    }

    fun volumeDown() {
        volumeKeyController.step(up = false)
    }

    fun playPause() {
        val id = selectedPlayer.value?.playerId ?: return
        // An offline pause is a pause whatever MA last reported, which the toggle would read.
        val pausedOffline = playPauseButton.value.pausableOffline
        val willPlay = !pausedOffline && selectedPlayer.value?.state != PlaybackState.PLAYING
        Log.i(TAG, "Play/pause from the mini player: ${if (willPlay) "play" else "pause"}")
        viewModelScope.launch {
            runCatching {
                if (pausedOffline) playerRepository.pause(id) else playerRepository.playPause(id)
            }
        }
    }

    fun previous() {
        val id = selectedPlayer.value?.playerId ?: return
        viewModelScope.launch { runCatching { playerRepository.previous(id) } }
    }

    fun next() {
        val id = selectedPlayer.value?.playerId ?: return
        viewModelScope.launch { runCatching { playerRepository.next(id) } }
    }
}
