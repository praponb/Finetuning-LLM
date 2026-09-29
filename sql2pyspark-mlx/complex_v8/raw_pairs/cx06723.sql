WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE is_skipped = TRUE
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        days_since_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY MONTH(played_at) ORDER BY ms_played ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        LAG(ms_played) OVER w_played_at_month_by_ms_played_2 AS prev_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, is_skipped ORDER BY days_since_played_at), 3) AS days_since_played_at_percent_rank,
        LAG(played_at, 3) OVER (PARTITION BY track_id, is_skipped ORDER BY days_since_played_at, id) AS prev3_played_at,
        SUM(days_since_played_at) OVER (PARTITION BY MONTH(played_at) ORDER BY ms_played ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_played_at_sum_6_prior,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY track_id ORDER BY days_since_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device, source), 0), 4) AS ms_played_share_of_device_source,
        LAST_VALUE(days_since_played_at) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY days_since_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY days_since_played_at), 3) AS days_since_played_at_percent_rank_3,
        ms_played - LAG(ms_played) OVER (PARTITION BY MONTH(played_at) ORDER BY ms_played ASC, id) AS ms_played_change,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id, device)) AS distinct_listener_id_in_track_id_device,
        COUNT(*) OVER (PARTITION BY track_id, device) AS rows_in_track_id_device,
        ROUND(AVG(days_since_played_at) OVER (PARTITION BY MONTH(played_at) ORDER BY ms_played ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS days_since_played_at_avg_2_prior
    FROM valid_plays
    WINDOW w_played_at_month_by_ms_played_2 AS (PARTITION BY MONTH(played_at) ORDER BY ms_played ASC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        prev_ms_played + ms_played_avg_2 AS prev_ms_played_plus_ms_played_avg_2
    FROM windowed
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        distinct_listener_id_in_track_id_device,
        COUNT(DISTINCT device) AS distinct_devices,
        ROUND(STDDEV(ms_played_share_of_device_source), 2) AS stddev_ms_played_share_of_device_source,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true
    FROM annotated
    GROUP BY listener_id, distinct_listener_id_in_track_id_device
    HAVING MAX(ms_played) > 130888
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY distinct_devices, listener_id, distinct_listener_id_in_track_id_device) AS rn,
        MIN(n_is_skipped_true) OVER (PARTITION BY n_is_skipped_true, distinct_devices) AS n_is_skipped_true_distinct_devices_min_n_is_skip,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id, distinct_devices ORDER BY distinct_devices), 3) AS distinct_devices_percent_rank
    FROM grouped
)
-- Final projection
SELECT
    listener_id,
    distinct_listener_id_in_track_id_device,
    stddev_ms_played_share_of_device_source,
    rn,
    n_is_skipped_true_distinct_devices_min_n_is_skip,
    distinct_devices_percent_rank
FROM with_windows
