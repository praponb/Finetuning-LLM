WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE status IN ('ACTIVE', 'LAPSED', 'CANCELLED')
      AND product IN ('HEALTH', 'AUTO', 'HOME')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(days_start_date_to_end_date) OVER (ORDER BY days_start_date_to_end_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_start_date_to_end_date_avg_2,
        premium - LAG(premium) OVER w_holder_id_start_date_month_b AS premium_change,
        ROW_NUMBER() OVER (ORDER BY start_date DESC, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, MONTH(start_date) ORDER BY premium), 3) AS premium_percent_rank,
        DENSE_RANK() OVER w_start_date_month_product_by AS premium_dense_rank,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id, product ORDER BY days_start_date_to_end_date, id)) AS days_since_prev_start_date,
        ROUND(days_start_date_to_end_date - AVG(days_start_date_to_end_date) OVER (PARTITION BY holder_id, product), 2) AS days_start_date_to_end_date_vs_holder_id_product,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY days_start_date_to_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        LEAD(start_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product ORDER BY premium, id) AS next2_start_date,
        DENSE_RANK() OVER (PARTITION BY holder_id, product ORDER BY days_start_date_to_end_date) AS days_start_date_to_end_date_dense_rank,
        LAST_VALUE(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        SUM(days_start_date_to_end_date) OVER w_holder_id_product AS holder_id_product_sum_days_start_date_to_end_dat,
        MIN(premium) OVER (PARTITION BY holder_id, product) AS holder_id_product_min_premium,
        LEAD(days_start_date_to_end_date) OVER (PARTITION BY holder_id, MONTH(start_date) ORDER BY premium, id) AS next_days_start_date_to_end_date,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product ORDER BY premium, id) AS premium_rank
    FROM valid_policys
    WINDOW w_holder_id_start_date_month_b AS (PARTITION BY holder_id, MONTH(start_date) ORDER BY premium, id), w_start_date_month_product_by AS (PARTITION BY DATE_TRUNC('MONTH', start_date), product ORDER BY premium, id), w_holder_id_product AS (PARTITION BY holder_id, product)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM ranked
    WHERE days_start_date_to_end_date_dense_rank <= 2
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    product_clean,
    days_start_date_to_end_date_avg_2,
    premium_change,
    premium_dense_rank,
    days_since_prev_start_date,
    days_start_date_to_end_date_vs_holder_id_product,
    first_known_start_date,
    next2_start_date,
    last_premium,
    holder_id_product_sum_days_start_date_to_end_dat,
    next_days_start_date_to_end_date,
    premium_rank
FROM top_per_group
WHERE days_start_date_to_end_date <= 210
ORDER BY days_since_prev_start_date DESC NULLS FIRST
