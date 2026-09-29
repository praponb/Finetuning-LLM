WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        ABS(ms_played - 112786) AS ms_played_distance
    FROM plays
    WHERE source = 'search'
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.source_prefix,
        s.ms_played_distance,
        p.id AS play_id,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM recent_plays s
    JOIN plays p ON p.track_id = s.track_id AND p.device IS NOT NULL
)
SELECT *
FROM (
    SELECT listener_id, track_id, play_device, ms_played_distance
    FROM with_children_plays
) src
PIVOT (
    SUM(ms_played_distance)
    FOR play_device IN ('phone', 'desktop')
)
