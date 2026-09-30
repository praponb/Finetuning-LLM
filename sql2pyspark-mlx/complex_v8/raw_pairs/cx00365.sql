WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m,
        CASE WHEN device IN ('speaker', 'phone') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE track_id IS NOT NULL
      AND ms_played < 223262
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id, device), 2) AS ms_played_vs_listener_id_device_avg,
        RANK() OVER w_all_by_played_at_plus_15m AS played_at_plus_15m_rank,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id)) AS distinct_listener_id_in_track_id,
        MAX(ms_played) OVER (PARTITION BY listener_id, device) AS listener_id_device_max_ms_played,
        NTILE(3) OVER (PARTITION BY device_group ORDER BY played_at_plus_15m, id) AS played_at_plus_15m_tercile,
        ROUND(ms_played - AVG(ms_played) OVER w_track_id, 2) AS ms_played_vs_track_id_avg,
        SUM(ms_played) OVER (PARTITION BY listener_id, device) AS listener_id_device_sum_ms_played,
        COUNT(*) OVER (PARTITION BY listener_id, device) AS rows_in_listener_id_device,
        FIRST_VALUE(ms_played) OVER (PARTITION BY listener_id, device_group ORDER BY played_at_plus_15m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        MAX(ms_played) OVER w_track_id AS track_id_max_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, device_group ORDER BY played_at_plus_15m, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        DENSE_RANK() OVER w_all_by_played_at_plus_15m_ti AS played_at_plus_15m_dense_rank,
        MIN(ms_played) OVER (PARTITION BY listener_id, device_group ORDER BY played_at_plus_15m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY played_at_plus_15m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played
    FROM base
    WINDOW w_all_by_played_at_plus_15m AS (ORDER BY played_at_plus_15m DESC, id), w_track_id AS (PARTITION BY track_id), w_listener_id_device AS (PARTITION BY listener_id, device), w_all_by_played_at_plus_15m_ti AS (ORDER BY played_at_plus_15m DESC)
)
SELECT
    id,
    listener_id,
    track_id,
    source,
    device_group,
    ms_played_vs_listener_id_device_avg,
    played_at_plus_15m_rank,
    distinct_listener_id_in_track_id,
    listener_id_device_max_ms_played,
    played_at_plus_15m_tercile,
    ms_played_vs_track_id_avg,
    listener_id_device_sum_ms_played,
    rows_in_listener_id_device,
    first_ms_played,
    track_id_max_ms_played,
    ms_played_avg_2,
    played_at_plus_15m_dense_rank,
    min_ms_played_so_far,
    second_ms_played,
    LOWER(TRIM(device)) AS device_clean,
    CONCAT_WS(': ', device_group, device) AS device_group_device_label
FROM metrics
