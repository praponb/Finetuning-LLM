WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_ADD(start_date, 365) AS start_date_plus_365d
    FROM policies
    WHERE (end_date < DATE '2026-09-23' OR premium BETWEEN 918.1 AND 4906.3)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        ROUND(CUME_DIST() OVER w_start_date_month_by_start_da, 3) AS start_date_cume_dist,
        MAX(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date_plus_365d), status ORDER BY end_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        FIRST_VALUE(start_date) OVER (PARTITION BY product ORDER BY start_date_plus_365d ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        LAG(end_date, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC NULLS FIRST, id) AS prev3_end_date,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_sum_2,
        NTILE(3) OVER (PARTITION BY product ORDER BY start_date_plus_365d ASC, id) AS start_date_plus_365d_tercile,
        SUM(premium) OVER (PARTITION BY product, DATE_TRUNC('MONTH', end_date) ORDER BY premium, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_sum_2_prior,
        NTILE(4) OVER w_start_date_plus_365d_month_s_2 AS end_date_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC NULLS FIRST, id), 3) AS start_date_percent_rank,
        MIN(premium) OVER (PARTITION BY status, holder_id) AS status_holder_id_min_premium
    FROM scoped_policys
    WINDOW w_start_date_month_by_start_da AS (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date DESC NULLS FIRST, id), w_start_date_plus_365d_month_s_2 AS (PARTITION BY DATE_TRUNC('MONTH', start_date_plus_365d), status ORDER BY end_date DESC, id)
),
totals AS (
    -- Group-level totals
    SELECT
        holder_id,
        DATE_TRUNC('MONTH', first_start_date) AS first_start_date_month,
        MAX(first_start_date) AS last_first_start_date,
        COUNT(*) AS n_rows,
        ROUND(SUM(premium), 2) AS total_premium,
        ROUND(SUM(start_date_percent_rank) / NULLIF(SUM(premium_sum_2), 0), 3) AS start_date_percent_rank_to_premium_sum_2_ratio,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        ROUND(100 * COUNT(CASE WHEN product NOT IN ('HOME', 'TRAVEL') THEN 1 END) / COUNT(*), 1) AS pct_product_not_in_home_travel
    FROM windowed
    WHERE premium_sum_2 < 10560.0
    GROUP BY holder_id, DATE_TRUNC('MONTH', first_start_date)
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_last_first_start_date AS last_first_start_date_dense_rank,
        MAX(n_rows) OVER (PARTITION BY distinct_holder_ids) AS distinct_holder_ids_max_n_rows
    FROM totals
    WINDOW w_all_by_last_first_start_date AS (ORDER BY last_first_start_date DESC)
)
SELECT
    holder_id,
    first_start_date_month,
    n_rows,
    total_premium,
    start_date_percent_rank_to_premium_sum_2_ratio,
    distinct_holder_ids,
    pct_product_not_in_home_travel,
    last_first_start_date_dense_rank,
    distinct_holder_ids_max_n_rows,
    COALESCE(distinct_holder_ids, 0) AS distinct_holder_ids_filled
FROM windowed_2
ORDER BY first_start_date_month
