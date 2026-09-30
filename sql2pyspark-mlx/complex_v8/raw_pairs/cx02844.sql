WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at)), 2) AS ms_played_vs_played_at_month_avg,
        ROUND(AVG(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at)), 2) AS played_at_month_avg_ms_played,
        SUM(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ms_played_sum_1_prior,
        RANK() OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id) AS ms_played_rank,
        LEAD(ms_played) OVER (PARTITION BY YEAR(played_at) ORDER BY ms_played ASC, id) AS next_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id), 3) AS ms_played_percent_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        MIN(ms_played) OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device)) AS distinct_listener_id_in_device,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY YEAR(played_at) ORDER BY ms_played ASC, id))) / 60, 1) AS minutes_since_prev_played_at
    FROM base_plays
)
SELECT
    id,
    track_id,
    ms_played,
    device,
    source,
    second_played_at,
    ms_played_vs_played_at_month_avg,
    played_at_month_avg_ms_played,
    ms_played_rank,
    ms_played_percent_rank,
    ms_played_avg_3_prior,
    min_ms_played_so_far,
    minutes_since_prev_played_at,
    CONCAT_WS(': ', device, source) AS device_source_label
FROM with_windows
ORDER BY ms_played_avg_3_prior
