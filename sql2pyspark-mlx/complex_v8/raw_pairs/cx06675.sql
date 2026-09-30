WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE listener_id >= 4
      AND ms_played BETWEEN 92277 AND 324058
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(played_at) OVER (ORDER BY played_at NULLS LAST, id) AS next_played_at,
        SUM(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_sum_2,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY track_id, source ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        SUM(ms_played) OVER (PARTITION BY device, MONTH(played_at)) AS device_played_at_month_sum_ms_played,
        ROUND(AVG(ms_played) OVER w_device_played_at_month, 2) AS device_played_at_month_avg_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id, 0), 4) AS ms_played_share_of_listener_id,
        SUM(ms_played) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        NTILE(3) OVER (ORDER BY ms_played, id) AS ms_played_tercile,
        ROW_NUMBER() OVER w_all_by_ms_played_2 AS ms_played_seq,
        MAX(ms_played) OVER (PARTITION BY track_id, source ORDER BY ms_played ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_max_2_prior,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device, MONTH(played_at)), 2) AS ms_played_vs_device_played_at_month_avg,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_all_by_played_at)) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_track_id_device, 0), 4) AS ms_played_share_of_track_id_device,
        RANK() OVER (ORDER BY ms_played) AS ms_played_rank
    FROM prepared
    WINDOW w_device_played_at_month AS (PARTITION BY device, MONTH(played_at)), w_listener_id AS (PARTITION BY listener_id), w_all_by_ms_played_2 AS (ORDER BY ms_played DESC, id), w_all_by_played_at AS (ORDER BY played_at NULLS LAST, id), w_track_id_device AS (PARTITION BY track_id, device)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        ms_played_rank,
        DATE_TRUNC('MONTH', next_played_at) AS next_played_at_month,
        ms_played_seq,
        MIN(played_at) AS first_played_at,
        COUNT(DISTINCT track_id) AS distinct_track_ids
    FROM ranked
    GROUP BY ms_played_rank, DATE_TRUNC('MONTH', next_played_at), ms_played_seq
)
-- Final projection
SELECT ms_played_rank, next_played_at_month, ms_played_seq
FROM summary
WHERE ms_played_rank < 8
ORDER BY ms_played_rank, ms_played_seq DESC
