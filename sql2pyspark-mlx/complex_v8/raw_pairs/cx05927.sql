WITH scoped_plays AS (
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
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE track_id >= 18
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(days_since_played_at) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS days_since_played_at_max_3,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id, device ORDER BY days_since_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        SUM(ms_played) OVER (PARTITION BY device) AS device_sum_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY track_id)) AS distinct_device_in_track_id,
        ROUND(AVG(days_since_played_at) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_played_at_avg_3,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        ROUND(AVG(days_since_played_at) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_played_at_avg_3_2,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id), 2) AS ms_played_vs_track_id_avg
    FROM scoped_plays
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', device, source) AS device_source_label,
        LEAST(second_ms_played, last_known_ms_played) AS least_second_ms_played_last_known_ms_played
    FROM windowed
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_days_since_played_at AS days_since_played_at_dense_rank,
        SUM(days_since_played_at) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_played_at,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id) AS distinct_listener_id_in_track_id
    FROM annotated
    WINDOW w_all_by_days_since_played_at AS (ORDER BY days_since_played_at DESC, id), w_track_id AS (PARTITION BY track_id)
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    days_since_played_at,
    device_sum_ms_played,
    last_known_ms_played,
    distinct_device_in_track_id,
    days_since_played_at_avg_3,
    days_since_played_at_avg_3_2,
    device_source_label,
    least_second_ms_played_last_known_ms_played,
    days_since_played_at_dense_rank,
    running_days_since_played_at,
    distinct_listener_id_in_track_id,
    DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
    ROUND(ms_played / NULLIF(device_sum_ms_played, 0), 3) AS ms_played_per_device_sum_ms_played
FROM metrics
ORDER BY distinct_device_in_track_id, least_second_ms_played_last_known_ms_played NULLS LAST, id
LIMIT 10
