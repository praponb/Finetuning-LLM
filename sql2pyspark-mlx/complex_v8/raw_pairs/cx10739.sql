WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        UPPER(TRIM(device)) AS device_clean
    FROM plays
    WHERE is_skipped = TRUE
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM filtered_plays s
    LEFT JOIN plays p ON p.track_id = s.track_id AND TO_DATE(p.played_at) < DATE '2025-02-18'
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.play_id,
        s.play_source,
        p.id AS play_id_2,
        p.track_id AS play_track_id,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM exploded_detail_plays s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    track_id,
    play_id,
    play_source,
    play_track_id,
    play_is_skipped,
    play_ms_played,
    COALESCE(play_source, 'UNKNOWN') AS play_source_or_unknown
FROM line_level_plays
WHERE listener_id <= 11
ORDER BY listener_id, track_id
