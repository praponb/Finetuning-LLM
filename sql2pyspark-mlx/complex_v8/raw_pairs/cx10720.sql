WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE MONTH(played_at) IN (2, 3)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ms_played_bucket - LAG(ms_played_bucket) OVER (ORDER BY played_at DESC, id) AS ms_played_bucket_change,
        SUM(ms_played_bucket) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played_bucket,
        FIRST_VALUE(ms_played) OVER (PARTITION BY listener_id, source ORDER BY source_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        MAX(ms_played) OVER (PARTITION BY listener_id, source ORDER BY source_len, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_max_4,
        NTH_VALUE(ms_played_bucket, 2) OVER (PARTITION BY source_len, device ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_bucket,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at DESC), 3) AS played_at_percent_rank,
        SUM(source_len) OVER (PARTITION BY source_len, device ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_source_len,
        ROW_NUMBER() OVER (PARTITION BY source_len, device ORDER BY ms_played_bucket, id) AS seq_no,
        ROUND(AVG(source_len) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_source_len,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id, 0), 4) AS ms_played_share_of_listener_id,
        SUM(ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played
    FROM filtered
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source_len,
        ms_played_bucket,
        ms_played_bucket_change,
        first_ms_played,
        played_at_percent_rank,
        running_source_len,
        seq_no,
        minutes_since_prev_played_at,
        ms_played_share_of_listener_id,
        MIN(ms_played_bucket_change) OVER (PARTITION BY source) AS source_min_ms_played_bucket_change,
        DENSE_RANK() OVER (PARTITION BY is_skipped, device ORDER BY ms_played_share_of_listener_id ASC, id) AS ms_played_share_of_listener_id_dense_rank,
        FIRST_VALUE(first_ms_played) OVER (PARTITION BY is_skipped, device ORDER BY ms_played_share_of_listener_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_ms_played,
        DENSE_RANK() OVER (PARTITION BY source, device ORDER BY minutes_since_prev_played_at DESC) AS minutes_since_prev_played_at_dense_rank,
        minutes_since_prev_played_at - LAG(minutes_since_prev_played_at) OVER w_device_source_by_listener_id AS minutes_since_prev_played_at_change
    FROM with_trends
    WINDOW w_device_source_by_listener_id AS (PARTITION BY device, source ORDER BY listener_id_avg_source_len DESC, id)
)
-- Final projection
SELECT
    id,
    ms_played,
    is_skipped,
    source_len,
    ms_played_bucket,
    running_source_len,
    seq_no,
    ms_played_share_of_listener_id,
    source_min_ms_played_bucket_change,
    ms_played_share_of_listener_id_dense_rank,
    first_first_ms_played,
    minutes_since_prev_played_at_change,
    DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
    CASE WHEN first_ms_played >= 299017 THEN 'A' WHEN first_ms_played >= 72464 THEN 'B' ELSE 'C' END AS first_ms_played_tier
FROM with_trends_2
