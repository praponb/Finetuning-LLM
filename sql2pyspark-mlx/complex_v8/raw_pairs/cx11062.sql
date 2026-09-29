WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj
    FROM policies
    WHERE YEAR(end_date) = 2026
      AND premium BETWEEN 2033.8 AND 3544.1
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_adj,
        p.segment AS holder_segment
    FROM base_policys s
    JOIN policyholders p ON p.id = s.holder_id AND p.region IN ('East', 'West')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium_adj - AVG(premium_adj) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date)), 2) AS premium_adj_vs_holder_id_end_date_month_avg,
        MAX(premium_adj) OVER (PARTITION BY YEAR(start_date), status ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_adj_max_3_prior,
        SIZE(COLLECT_SET(holder_id) OVER w_holder_segment) AS distinct_holder_id_in_holder_segment,
        RANK() OVER (PARTITION BY holder_id ORDER BY end_date ASC) AS end_date_rank,
        COUNT(*) OVER (PARTITION BY holder_segment) AS rows_in_holder_segment,
        MIN(premium_adj) OVER (PARTITION BY holder_segment, holder_id ORDER BY premium_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_adj_so_far,
        COUNT(*) OVER w_holder_id_end_date_month AS rows_in_holder_id_end_date_month,
        NTILE(4) OVER w_holder_id_by_end_date AS end_date_quartile,
        ROUND(CUME_DIST() OVER w_all_by_start_date, 3) AS start_date_cume_dist,
        DATEDIFF(start_date, LAG(start_date) OVER w_start_date_year_status_by_st_2) AS days_since_prev_start_date,
        RANK() OVER w_start_date_year_status_by_st_3 AS start_date_rank,
        LEAD(premium_adj) OVER (ORDER BY start_date, id) AS next_premium_adj,
        COUNT(*) OVER (PARTITION BY holder_id ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(premium_adj) IGNORE NULLS OVER (PARTITION BY holder_segment, holder_id ORDER BY premium_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_adj,
        ROUND(premium_adj - AVG(premium_adj) OVER (PARTITION BY holder_segment), 2) AS premium_adj_vs_holder_segment_avg,
        RANK() OVER w_all_by_end_date_ties AS end_date_rank_2
    FROM joined
    WINDOW w_holder_segment AS (PARTITION BY holder_segment), w_holder_id_end_date_month AS (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date)), w_holder_id_by_end_date AS (PARTITION BY holder_id ORDER BY end_date ASC, id), w_all_by_start_date AS (ORDER BY start_date DESC, id), w_start_date_year_status_by_st_2 AS (PARTITION BY YEAR(start_date), status ORDER BY start_date, id), w_start_date_year_status_by_st_3 AS (PARTITION BY YEAR(start_date), status ORDER BY start_date), w_all_by_end_date_ties AS (ORDER BY end_date NULLS LAST)
)
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    status,
    premium_adj,
    premium_adj_vs_holder_id_end_date_month_avg,
    premium_adj_max_3_prior,
    distinct_holder_id_in_holder_segment,
    rows_in_holder_segment,
    min_premium_adj_so_far,
    rows_in_holder_id_end_date_month,
    end_date_quartile,
    start_date_cume_dist,
    days_since_prev_start_date,
    start_date_rank,
    running_count,
    first_known_premium_adj,
    premium_adj_vs_holder_segment_avg,
    end_date_rank_2,
    CASE WHEN first_known_premium_adj >= 5675.0 THEN 'large' WHEN first_known_premium_adj >= 2390.0 THEN 'medium' ELSE 'small' END AS first_known_premium_adj_tier
FROM metrics
ORDER BY premium_adj_vs_holder_id_end_date_month_avg NULLS LAST
