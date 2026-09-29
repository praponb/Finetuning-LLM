WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE track_id IS NOT NULL
      AND source = 'playlist'
),
screened AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND LOWER(t.title) LIKE '% s%'
              AND SIZE(t.moods) >= 1
        )
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(ms_played) AS min_play_ms_played,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        COUNT(CASE WHEN device = 'speaker' THEN 1 END) AS play_device_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played,
        p.avg_play_ms_played,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM screened s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source_clean,
    min_play_ms_played,
    avg_play_ms_played,
    play_device_count
FROM summarized_rows
WHERE ms_played BETWEEN 92277 AND 251066
