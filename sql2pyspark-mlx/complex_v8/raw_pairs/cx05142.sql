WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE HOUR(played_at) BETWEEN 7 AND 10
      AND source IS NOT NULL
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.source_slug,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM cleaned s
    JOIN plays p ON p.listener_id = s.listener_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        COUNT(CASE WHEN device NOT IN ('speaker', 'car') THEN 1 END) AS play_device_count,
        MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_device_count, 0) AS play_device_count,
        p.first_played_at
    FROM detail_rows s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
-- Subtotals with CUBE
SELECT
    play_source,
    source,
    listener_id,
    MAX(play_played_at) AS last_play_played_at,
    COUNT(*) AS row_count,
    SUM(play_device_count) AS total_play_device_count
FROM with_counts
GROUP BY CUBE(play_source, source, listener_id)
ORDER BY play_source NULLS LAST, source NULLS LAST, listener_id NULLS LAST
