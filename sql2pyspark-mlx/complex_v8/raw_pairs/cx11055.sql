WITH scoped AS (
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
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE track_id >= 19
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM scoped
    WHERE ms_played > (SELECT AVG(ms_played) FROM scoped)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_bucket,
        is_skipped_flag,
        ABS(is_skipped_flag - 1) AS is_skipped_flag_distance
    FROM with_activity
    WHERE TO_DATE(played_at) < DATE '2025-02-18'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        track_id,
        device,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        ROUND(100 * COUNT(CASE WHEN device IN ('phone', 'tv') THEN 1 END) / COUNT(*), 1) AS pct_device_in_phone_tv,
        ROUND(100 * COUNT(CASE WHEN source NOT IN ('album', 'playlist') THEN 1 END) / COUNT(*), 1) AS pct_source_not_in_album_playlist,
        MIN(is_skipped_flag) AS min_is_skipped_flag
    FROM labeled
    WHERE is_skipped = TRUE
    GROUP BY 1, 2, 3
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM summary s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source NOT IN ('search', 'album')
              AND HOUR(p.played_at) BETWEEN 10 AND 15
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        track_id,
        device,
        played_at_month,
        pct_source_not_in_album_playlist,
        DENSE_RANK() OVER (ORDER BY pct_source_not_in_album_playlist NULLS LAST, track_id, device, played_at_month) AS pct_source_not_in_album_playlist_dense_rank,
        DENSE_RANK() OVER w_all_by_pct_source_not_in_alb_2 AS pct_source_not_in_album_playlist_dense_rank_2,
        SIZE(COLLECT_SET(track_id) OVER w_min_is_skipped_flag) AS distinct_track_id_in_min_is_skipped_flag,
        ROUND(AVG(min_is_skipped_flag) OVER (PARTITION BY min_is_skipped_flag), 2) AS min_is_skipped_flag_avg_min_is_skipped_flag,
        COUNT(*) OVER (PARTITION BY min_is_skipped_flag) AS rows_in_min_is_skipped_flag
    FROM screened
    WINDOW w_all_by_pct_source_not_in_alb_2 AS (ORDER BY pct_source_not_in_album_playlist NULLS LAST), w_min_is_skipped_flag AS (PARTITION BY min_is_skipped_flag)
)
-- Final projection
SELECT
    track_id,
    device,
    played_at_month,
    pct_source_not_in_album_playlist_dense_rank,
    pct_source_not_in_album_playlist_dense_rank_2,
    distinct_track_id_in_min_is_skipped_flag,
    rows_in_min_is_skipped_flag,
    TO_DATE(played_at_month) AS played_at_month_date,
    DATE_TRUNC('HOUR', played_at_month) AS played_at_month_hour
FROM windowed
WHERE TO_DATE(played_at_month) < DATE '2025-02-01'
ORDER BY device DESC
