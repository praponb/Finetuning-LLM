WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE premium <= 3817.5
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, product), 0), 4) AS premium_share_of_holder_id_product,
        ROUND(CUME_DIST() OVER (PARTITION BY product ORDER BY end_date ASC NULLS LAST, id), 3) AS end_date_cume_dist,
        DENSE_RANK() OVER (ORDER BY premium) AS premium_dense_rank,
        MIN(premium) OVER w_holder_id_product AS holder_id_product_min_premium,
        ROUND(AVG(premium) OVER (PARTITION BY product ORDER BY end_date ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        SUM(premium) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        DATEDIFF(end_date, LAG(end_date) OVER w_all_by_premium) AS days_since_prev_end_date,
        ROUND(AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS product_holder_id_avg_premium,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, product), 2) AS holder_id_product_avg_premium,
        LAG(premium, 1, 0) OVER w_holder_id_product_by_premium AS prev_premium
    FROM scoped_policys
    WINDOW w_holder_id_product AS (PARTITION BY holder_id, product), w_all_by_premium AS (ORDER BY premium, id), w_holder_id_product_by_premium AS (PARTITION BY holder_id, product ORDER BY premium, id)
),
derived AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date
    FROM sequenced
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        premium_dense_rank,
        holder_id,
        product,
        SUM(days_since_end_date) AS total_days_since_end_date,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(DISTINCT product) AS distinct_products,
        COUNT(*) AS n_rows,
        MAX(end_date) AS last_end_date,
        MIN(premium_dense_rank) AS min_premium_dense_rank
    FROM derived
    GROUP BY premium_dense_rank, holder_id, product
)
-- Final projection
SELECT
    premium_dense_rank,
    holder_id,
    product,
    total_days_since_end_date,
    distinct_holder_ids,
    distinct_products,
    n_rows,
    last_end_date,
    min_premium_dense_rank,
    SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
    LENGTH(TRIM(product)) AS product_len
FROM grouped
WHERE distinct_products >= 3
ORDER BY min_premium_dense_rank DESC NULLS FIRST, last_end_date DESC
