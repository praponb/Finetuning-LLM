WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE premium < 3817.5
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY end_date, id) AS end_date_quartile,
        MAX(premium) OVER (PARTITION BY product ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_max_4,
        SUM(premium) OVER (PARTITION BY product) AS product_sum_premium,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY premium DESC, id) AS premium_dense_rank,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product), 2) AS premium_vs_product_avg,
        LAG(start_date) OVER (PARTITION BY product ORDER BY premium DESC, id) AS prev_start_date,
        ROUND(AVG(premium) OVER (PARTITION BY product), 2) AS product_avg_premium,
        ROW_NUMBER() OVER (ORDER BY premium DESC NULLS FIRST, id) AS rn,
        RANK() OVER (PARTITION BY product ORDER BY premium DESC) AS premium_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product)) AS distinct_status_in_product
    FROM prepared
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    end_date_month,
    product_prefix,
    premium_dense_rank,
    prev_start_date,
    product_avg_premium,
    rn,
    premium_rank,
    distinct_status_in_product,
    CONCAT_WS(': ', product, product_prefix) AS product_product_prefix_label
FROM ranked
ORDER BY product_avg_premium DESC NULLS FIRST, id
LIMIT 5
