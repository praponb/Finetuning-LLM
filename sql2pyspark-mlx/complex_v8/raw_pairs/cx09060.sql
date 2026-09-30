WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref
    FROM plays
    WHERE track_id IS NOT NULL
      AND source IN ('playlist', 'search')
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE device IN ('speaker', 'car')) AS play_device_count,
        COUNT(*) FILTER (WHERE device IS NOT NULL) AS play_device_count_2
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_device_count, 0) AS play_device_count,
        COALESCE(p.play_device_count_2, 0) AS play_device_count_2
    FROM cleaned s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
SELECT id, measure, reading
FROM (
    SELECT id, play_device_count_2, play_device_count, ms_played
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (play_device_count_2, play_device_count, ms_played)
) unp
