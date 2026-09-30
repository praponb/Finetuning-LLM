WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE track_id IS NOT NULL
      AND device = 'tv'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_3,
        MAX(ms_played) OVER w_source AS source_max_ms_played,
        ms_played - LAG(ms_played) OVER w_device_by_ms_played AS ms_played_change,
        RANK() OVER w_source_is_skipped_by_ms_play AS ms_played_rank,
        MAX(ms_played) OVER (PARTITION BY device, track_id ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        FIRST_VALUE(played_at) OVER (PARTITION BY device, track_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY source, is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY device, track_id ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at
    FROM recent
    WINDOW w_source AS (PARTITION BY source), w_device_by_ms_played AS (PARTITION BY device ORDER BY ms_played, id), w_source_is_skipped_by_ms_play AS (PARTITION BY source, is_skipped ORDER BY ms_played DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE ms_played_rank = 1
      AND source_max_ms_played < 2202250
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ms_played - LAG(ms_played) OVER (PARTITION BY ms_played_rank ORDER BY minutes_since_prev_played_at, id) AS ms_played_change_2,
        SUM(ms_played) OVER (ORDER BY source_max_ms_played, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_sum_3_prior,
        SIZE(COLLECT_SET(track_id) OVER w_played_at_month) AS distinct_track_id_in_played_at_month,
        FIRST_VALUE(first_played_at) IGNORE NULLS OVER (PARTITION BY ms_played_rank ORDER BY minutes_since_prev_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_played_at,
        ROUND(AVG(minutes_since_prev_played_at) OVER (PARTITION BY ms_played_rank ORDER BY minutes_since_prev_played_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS minutes_since_prev_played_at_avg_3_prior,
        MAX(ms_played) OVER (PARTITION BY MONTH(played_at)) AS played_at_month_max_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY MONTH(played_at)), 2) AS ms_played_vs_played_at_month_avg
    FROM top_rows
    WINDOW w_played_at_month AS (PARTITION BY MONTH(played_at))
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    ms_played_avg_3,
    source_max_ms_played,
    ms_played_change,
    ms_played_rank,
    ms_played_max_7,
    first_known_ms_played,
    minutes_since_prev_played_at,
    ms_played_change_2,
    ms_played_sum_3_prior,
    distinct_track_id_in_played_at_month,
    first_known_first_played_at,
    minutes_since_prev_played_at_avg_3_prior,
    played_at_month_max_ms_played,
    ms_played_vs_played_at_month_avg,
    ROUND(ms_played_avg_3 * 1.1, 2) AS ms_played_avg_3_adj
FROM metrics_2
ORDER BY minutes_since_prev_played_at_avg_3_prior DESC
