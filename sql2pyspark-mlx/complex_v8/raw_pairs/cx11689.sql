WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM policies
    WHERE premium <= 2844.1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        days_since_end_date,
        NTILE(4) OVER w_product_by_days_since_end_da AS days_since_end_date_quartile,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY days_since_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        ROUND(AVG(premium) OVER (PARTITION BY product, holder_id ORDER BY days_since_end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        ROUND(CUME_DIST() OVER w_product_by_start_date_ties, 3) AS start_date_cume_dist,
        RANK() OVER w_product_by_days_since_end_da_2 AS days_since_end_date_rank,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY days_since_end_date DESC, id) AS days_since_end_date_dense_rank,
        LEAD(end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), YEAR(start_date) ORDER BY start_date ASC NULLS LAST, id) AS next_end_date,
        SIZE(COLLECT_SET(product) OVER w_holder_id) AS distinct_product_in_holder_id,
        ROUND(days_since_end_date - AVG(days_since_end_date) OVER (PARTITION BY holder_id), 2) AS days_since_end_date_vs_holder_id_avg,
        LAST_VALUE(start_date) OVER (PARTITION BY product ORDER BY days_since_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product ORDER BY days_since_end_date DESC), 3) AS days_since_end_date_percent_rank,
        MAX(days_since_end_date) OVER (PARTITION BY product, holder_id ORDER BY days_since_end_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_end_date_max_4,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY product ORDER BY days_since_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, DATE_TRUNC('MONTH', start_date))) AS distinct_holder_id_in_product_start_date_month,
        SUM(days_since_end_date) OVER (PARTITION BY holder_id) AS holder_id_sum_days_since_end_date
    FROM scoped_policys
    WINDOW w_product_by_days_since_end_da AS (PARTITION BY product ORDER BY days_since_end_date DESC, id), w_product_by_start_date_ties AS (PARTITION BY product ORDER BY start_date ASC), w_product_by_days_since_end_da_2 AS (PARTITION BY product ORDER BY days_since_end_date DESC), w_holder_id AS (PARTITION BY holder_id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium_avg_7 - AVG(premium_avg_7) OVER (PARTITION BY product, distinct_product_in_holder_id), 2) AS premium_avg_7_vs_product_distinct_product_in_hol,
        MAX(days_since_end_date_rank) OVER (ORDER BY days_since_end_date_dense_rank ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_end_date_rank_max_1_prior,
        SIZE(COLLECT_SET(holder_id) OVER w_product_distinct_product_in) AS distinct_holder_id_in_product_distinct_product_i,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_end_date), product ORDER BY last_start_date DESC, id) AS last_start_date_quintile
    FROM with_trends
    WINDOW w_product_distinct_product_in AS (PARTITION BY product, distinct_product_in_holder_id)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    days_since_end_date,
    last_known_premium,
    premium_avg_7,
    start_date_cume_dist,
    next_end_date,
    distinct_product_in_holder_id,
    days_since_end_date_percent_rank,
    days_since_end_date_max_4,
    last_known_end_date,
    premium_avg_7_vs_product_distinct_product_in_hol,
    days_since_end_date_rank_max_1_prior,
    ROUND(start_date_cume_dist / NULLIF(last_known_premium, 0), 3) AS start_date_cume_dist_per_last_known_premium
FROM ranked
ORDER BY last_known_end_date DESC NULLS FIRST, premium_avg_7_vs_product_distinct_product_in_hol DESC
