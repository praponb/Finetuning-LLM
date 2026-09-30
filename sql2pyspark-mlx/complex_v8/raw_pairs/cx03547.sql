WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        ROUND(premium * 0.9, 2) AS premium_adj,
        ABS(premium - 2824.5) AS premium_distance
    FROM policies
    WHERE status NOT IN ('EXPIRED', 'ACTIVE')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(premium) OVER (PARTITION BY status, holder_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY status ORDER BY premium_distance DESC NULLS FIRST, id)) AS days_since_prev_start_date,
        SUM(premium_adj) OVER (PARTITION BY status) AS status_sum_premium_adj,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium_distance DESC NULLS FIRST, id) AS premium_distance_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(start_date), status ORDER BY start_date DESC, id), 3) AS start_date_percent_rank,
        MAX(premium_adj) OVER (PARTITION BY status, holder_id ORDER BY premium_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_adj_max_2_prior,
        NTILE(3) OVER (PARTITION BY status, holder_id ORDER BY start_date, id) AS start_date_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(start_date), status ORDER BY start_date DESC), 3) AS start_date_cume_dist,
        LAG(premium_distance) OVER (PARTITION BY status, holder_id ORDER BY start_date, id) AS prev_premium_distance,
        LEAD(premium_adj, 2) OVER w_status_holder_id_by_premium_2 AS next2_premium_adj,
        LAG(premium) OVER (PARTITION BY status, holder_id ORDER BY start_date, id) AS prev_premium
    FROM cleaned
    WINDOW w_status_holder_id_by_premium_2 AS (PARTITION BY status, holder_id ORDER BY premium_adj ASC NULLS LAST, id), w_status AS (PARTITION BY status)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        premium_adj,
        premium_distance,
        premium_avg_3,
        days_since_prev_start_date,
        status_sum_premium_adj,
        rows_in_status,
        start_date_percent_rank,
        premium_adj_max_2_prior,
        start_date_tercile,
        start_date_cume_dist,
        prev_premium_distance,
        next2_premium_adj,
        prev_premium
    FROM windowed
    WHERE premium_distance_seq = 1
      AND start_date IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        premium,
        start_date,
        premium_adj,
        premium_avg_3,
        start_date_tercile,
        start_date_cume_dist,
        prev_premium_distance,
        next2_premium_adj,
        prev_premium,
        ROUND(CUME_DIST() OVER (ORDER BY next2_premium_adj ASC), 3) AS next2_premium_adj_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY start_date_tercile ORDER BY start_date_tercile ASC NULLS LAST, id), 3) AS start_date_tercile_percent_rank,
        LEAD(next2_premium_adj) OVER w_all_by_next2_premium_adj AS next_next2_premium_adj,
        ROUND(CUME_DIST() OVER (PARTITION BY start_date_tercile ORDER BY start_date_tercile ASC NULLS LAST), 3) AS start_date_tercile_cume_dist,
        MAX(next2_premium_adj) OVER (ORDER BY next2_premium_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_next2_premium_adj_so_far,
        ROUND(prev_premium_distance - AVG(prev_premium_distance) OVER (PARTITION BY holder_id, status), 2) AS prev_premium_distance_vs_holder_id_status_avg,
        RANK() OVER (ORDER BY next2_premium_adj ASC) AS next2_premium_adj_rank
    FROM top_per_group
    WINDOW w_all_by_next2_premium_adj AS (ORDER BY next2_premium_adj ASC, id)
)
-- Final projection
SELECT
    id,
    premium,
    premium_adj,
    premium_avg_3,
    start_date_tercile,
    prev_premium,
    start_date_tercile_percent_rank,
    start_date_tercile_cume_dist,
    TRUNC(start_date, 'MM') AS start_date_month_start,
    premium_avg_3 + start_date_cume_dist AS premium_avg_3_plus_start_date_cume_dist
FROM ranked
WHERE next2_premium_adj <= 5310.0
ORDER BY premium_avg_3_plus_start_date_cume_dist, start_date_tercile_cume_dist DESC
