WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE NOT is_skipped
      AND listener_id <= 15
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY track_id, source)) AS distinct_device_in_track_id_source,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        RANK() OVER w_source_listener_id_by_ms_pla AS ms_played_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, source), 2) AS ms_played_vs_track_id_source_avg,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id_source) AS distinct_listener_id_in_track_id_source,
        COUNT(*) OVER (PARTITION BY device, track_id) AS rows_in_device_track_id,
        LEAD(ms_played) OVER w_all_by_ms_played AS next_ms_played,
        LAG(ms_played, 2, 0) OVER (ORDER BY ms_played ASC, id) AS prev2_ms_played,
        COUNT(*) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(ms_played) OVER (PARTITION BY source, listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        MAX(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far
    FROM valid_plays
    WINDOW w_source_listener_id_by_ms_pla AS (PARTITION BY source, listener_id ORDER BY ms_played, id), w_track_id_source AS (PARTITION BY track_id, source), w_all_by_ms_played AS (ORDER BY ms_played, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY distinct_listener_id_in_track_id_source ORDER BY next_ms_played, id) AS next_ms_played_dense_rank,
        ROUND(AVG(running_count) OVER (PARTITION BY ms_played_rank ORDER BY rows_in_device_track_id DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS running_count_avg_1_prior,
        LEAD(ms_played_avg_6_prior) OVER (PARTITION BY is_skipped, track_id ORDER BY distinct_listener_id_in_track_id_source DESC, id) AS next_ms_played_avg_6_prior,
        SUM(rows_in_device_track_id) OVER (PARTITION BY track_id, distinct_listener_id_in_track_id_source) AS track_id_distinct_listener_id_in_track_id_source
    FROM sequenced
),
per_group AS (
    -- Summarise per group
    SELECT
        listener_id,
        is_skipped,
        device,
        ROUND(100 * COUNT(CASE WHEN device IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_device_is_not_null,
        MAX(max_ms_played_so_far) AS max_max_ms_played_so_far,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list
    FROM with_windows
    GROUP BY listener_id, is_skipped, device
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        listener_id,
        is_skipped,
        device,
        pct_device_is_not_null,
        max_max_ms_played_so_far,
        device_list,
        RANK() OVER (PARTITION BY device, is_skipped ORDER BY pct_device_is_not_null) AS pct_device_is_not_null_rank,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY is_skipped, listener_id)) AS distinct_device_in_is_skipped_listener_id
    FROM per_group
)
-- Final projection
SELECT
    listener_id,
    device,
    pct_device_is_not_null,
    device_list,
    pct_device_is_not_null_rank,
    distinct_device_in_is_skipped_listener_id,
    SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
    FLOOR(max_max_ms_played_so_far / 1000000) * 1000000 AS max_max_ms_played_so_far_bucket
FROM windowed
ORDER BY max_max_ms_played_so_far_bucket DESC NULLS FIRST, device DESC
