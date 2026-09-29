WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE track_id IS NOT NULL
      AND played_at < TIMESTAMP '2025-02-24 12:00:00'
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id <= 18
        )
      AND MONTH(s.played_at) IN (2, 3)
),
per_group AS (
    -- Group-level totals
    SELECT
        track_id,
        device,
        listener_id,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE device <> 'tv') AS n_device_tv,
        SUM(CASE WHEN device = 'tv' THEN 1 ELSE 0 END) AS n_device_tv_2,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS n_is_skipped_true
    FROM qualified
    GROUP BY 1, 2, 3
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.device = 'car' THEN 1 END) FROM plays p WHERE p.track_id = s.track_id) AS play_device_count,
        (SELECT COUNT(*) FILTER (WHERE NOT p.is_skipped) FROM plays p WHERE p.track_id = s.track_id) AS play_is_skipped_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM per_group s
)
-- Final projection
SELECT
    track_id,
    device,
    row_count,
    n_device_tv_2,
    play_device_count,
    play_is_skipped_count,
    play_count,
    ROUND(n_device_tv / NULLIF(play_count, 0), 3) AS n_device_tv_per_play_count
FROM with_stats
WHERE n_is_skipped_true >= 2
ORDER BY play_is_skipped_count DESC, n_device_tv_per_play_count DESC NULLS FIRST
