WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE track_id >= 19
      AND device IN ('speaker', 'desktop')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ms_played_bucket - LAG(ms_played_bucket) OVER w_source_device_len_by_ms_play AS ms_played_bucket_change,
        SUM(ms_played_bucket) OVER (PARTITION BY source, device_len ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_bucket,
        RANK() OVER (PARTITION BY track_id, listener_id ORDER BY ms_played_bucket DESC) AS ms_played_bucket_rank,
        DENSE_RANK() OVER w_source_device_len_by_ms_play_3 AS ms_played_bucket_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY device_len, device ORDER BY ms_played_bucket DESC, id) AS rn,
        device_len - LAG(device_len) OVER w_track_id_listener_id_by_ms_p_2 AS device_len_change,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY listener_id)) AS distinct_device_in_listener_id,
        DENSE_RANK() OVER (PARTITION BY device_len, device ORDER BY ms_played_bucket DESC) AS ms_played_bucket_dense_rank_2,
        LEAD(device_len) OVER (PARTITION BY source, device_len ORDER BY ms_played_bucket, id) AS next_device_len,
        SUM(ms_played_bucket) OVER (PARTITION BY source, device_len ORDER BY ms_played_bucket, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_bucket_sum_2,
        LEAD(ms_played_bucket, 2) OVER (PARTITION BY device_len, device ORDER BY ms_played_bucket DESC, id) AS next2_ms_played_bucket,
        ms_played_bucket - LAG(ms_played_bucket) OVER (PARTITION BY track_id, listener_id ORDER BY ms_played_bucket DESC, id) AS ms_played_bucket_change_2,
        MAX(ms_played_bucket) OVER (PARTITION BY source, device_len ORDER BY ms_played_bucket, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_bucket_max_2,
        LEAD(device_len, 1, 0) OVER (PARTITION BY source, device_len ORDER BY ms_played_bucket, id) AS next_device_len_2
    FROM cleaned_plays
    WINDOW w_source_device_len_by_ms_play AS (PARTITION BY source, device_len ORDER BY ms_played_bucket, id), w_source_device_len_by_ms_play_3 AS (PARTITION BY source, device_len ORDER BY ms_played_bucket), w_track_id_listener_id_by_ms_p_2 AS (PARTITION BY track_id, listener_id ORDER BY ms_played_bucket DESC, id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE ms_played_bucket_dense_rank_2 <= 3
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    ms_played_bucket,
    device_len,
    ms_played_bucket_change,
    ms_played_bucket_rank,
    ms_played_bucket_dense_rank,
    device_len_change,
    ms_played_bucket_dense_rank_2,
    next_device_len,
    ms_played_bucket_sum_2,
    next2_ms_played_bucket,
    ms_played_bucket_change_2,
    ms_played_bucket_max_2,
    next_device_len_2
FROM leaders
ORDER BY ms_played_bucket, ms_played_bucket_dense_rank, id
LIMIT 10
