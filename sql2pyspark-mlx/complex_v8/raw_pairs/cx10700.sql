WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE (ms_played <= 225733 OR source <> 'album')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(ms_played) OVER (PARTITION BY device) AS device_min_ms_played,
        LAG(played_at) OVER w_track_id_by_ms_played AS prev_played_at,
        ROUND(AVG(ms_played) OVER w_device, 2) AS device_avg_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device, MONTH(played_at) ORDER BY played_at DESC), 3) AS played_at_percent_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        LEAD(played_at) OVER w_device_played_at_month_by_pl_2 AS next_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device, MONTH(played_at) ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAG(ms_played) OVER (PARTITION BY source, track_id ORDER BY played_at ASC, id) AS prev_ms_played,
        RANK() OVER (PARTITION BY device, MONTH(played_at) ORDER BY played_at DESC) AS played_at_rank,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device)) AS distinct_track_id_in_device,
        FIRST_VALUE(ms_played) OVER (PARTITION BY source, track_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        LEAD(ms_played) OVER (PARTITION BY device, MONTH(played_at) ORDER BY played_at DESC, id) AS next_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY source, track_id ORDER BY played_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        LEAD(played_at, 3) OVER (PARTITION BY device, MONTH(played_at) ORDER BY played_at DESC, id) AS next3_played_at
    FROM base_plays
    WINDOW w_track_id_by_ms_played AS (PARTITION BY track_id ORDER BY ms_played DESC, id), w_device AS (PARTITION BY device), w_device_played_at_month_by_pl_2 AS (PARTITION BY device, MONTH(played_at) ORDER BY played_at DESC, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE played_at_rank <= 3
      AND played_at < TIMESTAMP '2025-02-18 12:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY device, source ORDER BY next_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(AVG(prev_ms_played) OVER (ORDER BY device_avg_ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS prev_ms_played_avg_6_prior,
        ROW_NUMBER() OVER w_all_by_device_avg_ms_played_2 AS device_avg_ms_played_seq,
        COUNT(*) OVER w_distinct_track_id_in_device AS rows_in_distinct_track_id_in_device_played_at_ra
    FROM top_per_group
    WINDOW w_all_by_device_avg_ms_played_2 AS (ORDER BY device_avg_ms_played DESC, id), w_distinct_track_id_in_device AS (PARTITION BY distinct_track_id_in_device, played_at_rank)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        YEAR(second_played_at) AS second_played_at_year,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        SUM(CASE WHEN device IS NOT NULL THEN 1 ELSE 0 END) AS n_device_is_not_null,
        SUM(rows_in_distinct_track_id_in_device_played_at_ra) AS total_rows_in_distinct_track_id_in_device_played
    FROM ranked
    WHERE played_at_percent_rank >= 0.4
    GROUP BY 1, 2
)
SELECT
    played_at_month,
    second_played_at_year,
    device_list,
    n_device_is_not_null,
    total_rows_in_distinct_track_id_in_device_played
FROM totals
ORDER BY second_played_at_year DESC NULLS FIRST, played_at_month
LIMIT 10
