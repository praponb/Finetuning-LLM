WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE device IS NOT NULL
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) AS play_is_skipped_count,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count,
        COALESCE(p.play_count, 0) AS play_count
    FROM scoped s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        device,
        COUNT(*) AS cnt,
        MAX(ms_played) AS max_ms_played,
        SUM(play_is_skipped_count) AS total_play_is_skipped_count,
        SUM(ms_played_bucket) AS total_ms_played_bucket
    FROM with_counts
    GROUP BY 1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE total_play_is_skipped_count > (SELECT AVG(total_play_is_skipped_count) FROM grouped)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM qualified
    WHERE total_play_is_skipped_count < (SELECT MAX(total_play_is_skipped_count) FROM qualified)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        device,
        ROUND(CUME_DIST() OVER w_device_by_total_ms_played_bu, 3) AS total_ms_played_bucket_cume_dist,
        MAX(total_ms_played_bucket) OVER (PARTITION BY cnt, device) AS cnt_device_max_total_ms_played_bucket,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY max_ms_played) AS max_ms_played_dense_rank,
        COUNT(*) OVER (PARTITION BY cnt, device) AS rows_in_cnt_device,
        ROUND(CUME_DIST() OVER (PARTITION BY device, cnt ORDER BY total_play_is_skipped_count DESC), 3) AS total_play_is_skipped_count_cume_dist
    FROM with_activity
    WINDOW w_device_by_total_ms_played_bu AS (PARTITION BY device ORDER BY total_ms_played_bucket DESC NULLS FIRST)
)
SELECT *
FROM (
    SELECT max_ms_played_dense_rank, device, total_ms_played_bucket_cume_dist
    FROM metrics
) src
PIVOT (
    MAX(total_ms_played_bucket_cume_dist) AS max_v, COUNT(total_ms_played_bucket_cume_dist) AS n
    FOR device IN ('tv', 'car', 'phone')
)
