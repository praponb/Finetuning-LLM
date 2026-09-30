WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
        CONCAT_WS(' / ', device, source) AS device_source_label
    FROM plays
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.device_ref,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM filtered s
    LEFT JOIN plays p ON p.listener_id = s.listener_id AND p.source <> 'playlist'
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    device_ref,
    play_id,
    play_played_at,
    play_source,
    play_device
FROM line_level
WHERE is_skipped = TRUE
ORDER BY device_ref, played_at DESC
