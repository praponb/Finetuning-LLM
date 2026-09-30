WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date IS NOT NULL
      AND product NOT IN ('AUTO', 'HEALTH')
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.segment AS holder_segment, p.holder_name
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        holder_segment,
        holder_name,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY premium ASC) AS premium_dense_rank,
        SIZE(COLLECT_SET(holder_id) OVER w_product_status) AS distinct_holder_id_in_product_status,
        RANK() OVER (ORDER BY end_date DESC) AS end_date_rank,
        LAG(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY end_date DESC, id) AS prev_start_date,
        LEAD(premium, 3, 0) OVER (PARTITION BY product ORDER BY premium ASC, id) AS next3_premium,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        MIN(premium) OVER w_holder_id_end_date_year AS holder_id_end_date_year_min_premium,
        SUM(premium) OVER (ORDER BY end_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_sum_2,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_segment), 2) AS premium_vs_holder_segment_avg,
        COUNT(*) OVER (PARTITION BY product, status) AS rows_in_product_status,
        MIN(premium) OVER (PARTITION BY holder_segment) AS holder_segment_min_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, YEAR(end_date)), 2) AS premium_vs_holder_id_end_date_year_avg,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_segment, 0), 4) AS premium_share_of_holder_segment,
        NTILE(10) OVER w_product_status_by_end_date AS end_date_decile,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, YEAR(end_date)), 0), 4) AS premium_share_of_holder_id_end_date_year
    FROM enriched_holder
    WINDOW w_product_status AS (PARTITION BY product, status), w_holder_id_end_date_year AS (PARTITION BY holder_id, YEAR(end_date)), w_holder_segment AS (PARTITION BY holder_segment), w_product_status_by_end_date AS (PARTITION BY product, status ORDER BY end_date DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        ROUND(AVG(next3_premium) OVER (PARTITION BY end_date_decile ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS next3_premium_avg_2_prior,
        NTILE(3) OVER (ORDER BY start_date DESC, id) AS start_date_tercile,
        ROUND(rows_in_product_status - AVG(rows_in_product_status) OVER (PARTITION BY holder_segment, status), 2) AS rows_in_product_status_vs_holder_segment_status_
    FROM metrics
),
per_group AS (
    -- Summarise per group
    SELECT
        premium_dense_rank,
        ROUND(SUM(last_known_premium), 2) AS total_last_known_premium,
        MAX(premium_share_of_holder_segment) AS max_premium_share_of_holder_segment,
        COUNT(DISTINCT product) AS distinct_products
    FROM with_windows
    GROUP BY premium_dense_rank
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(distinct_products) OVER (PARTITION BY distinct_products) AS distinct_products_max_distinct_products,
        LAG(total_last_known_premium, 2, 0) OVER (ORDER BY max_premium_share_of_holder_segment DESC NULLS FIRST, premium_dense_rank) AS prev2_total_last_known_premium
    FROM per_group
)
-- Final projection
SELECT
    premium_dense_rank,
    total_last_known_premium,
    max_premium_share_of_holder_segment,
    distinct_products_max_distinct_products,
    prev2_total_last_known_premium,
    CASE WHEN total_last_known_premium >= 18080.0 THEN 'large' WHEN total_last_known_premium >= 10630.0 THEN 'medium' ELSE 'small' END AS total_last_known_premium_tier
FROM ranked
