WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played BETWEEN 62294 AND 308104
      AND HOUR(played_at) BETWEEN 8 AND 14
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY source), 2) AS source_avg_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY ms_played_bucket NULLS LAST, id), 3) AS ms_played_bucket_cume_dist,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        ROUND(AVG(ms_played_bucket) OVER (PARTITION BY track_id, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_bucket_avg_3,
        MAX(ms_played_bucket) OVER (PARTITION BY source) AS source_max_ms_played_bucket,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY source)) AS distinct_listener_id_in_source,
        SUM(ms_played) OVER (PARTITION BY source) AS source_sum_ms_played,
        LAST_VALUE(ms_played_bucket) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_bucket,
        LAG(ms_played_bucket) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC NULLS FIRST, id) AS prev_ms_played_bucket,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY ms_played_bucket NULLS LAST), 3) AS ms_played_bucket_percent_rank,
        NTILE(10) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_decile
    FROM filtered
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(source_sum_ms_played / NULLIF(SUM(source_sum_ms_played) OVER (PARTITION BY distinct_listener_id_in_source, device), 0), 4) AS source_sum_ms_played_share_of_distinct_listener_,
        NTILE(5) OVER (ORDER BY source_max_ms_played_bucket, id) AS source_max_ms_played_bucket_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, distinct_listener_id_in_source ORDER BY source_max_ms_played_bucket DESC, id), 3) AS source_max_ms_played_bucket_cume_dist,
        SUM(ms_played_bucket_cume_dist) OVER (PARTITION BY distinct_listener_id_in_source, device) AS distinct_listener_id_in_source_device_sum_ms_pla
    FROM windowed
)
SELECT
    id,
    track_id,
    ms_played,
    source,
    ms_played_bucket,
    source_max_ms_played_bucket,
    source_sum_ms_played,
    prev_ms_played_bucket,
    ms_played_bucket_percent_rank,
    ms_played_decile,
    source_sum_ms_played_share_of_distinct_listener_,
    source_max_ms_played_bucket_quintile,
    source_max_ms_played_bucket_cume_dist,
    distinct_listener_id_in_source_device_sum_ms_pla,
    COALESCE(source_sum_ms_played, 0) AS source_sum_ms_played_filled
FROM with_trends
ORDER BY source DESC NULLS FIRST
