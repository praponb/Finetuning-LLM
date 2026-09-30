WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        line.product_id,
        line.qty
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE(items) AS line
        FROM carts
    ) t
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, SUM(product_id) OVER (PARTITION BY customer_id) AS customer_id_sum_product_id
    FROM flattened
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, b.country
    FROM metrics e
    INNER JOIN customers b
        ON e.customer_id = b.id
    WHERE b.name LIKE '%Pim%'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, qty - customer_id_sum_product_id AS qty_minus_customer_id_sum_product_id
    FROM with_dims
    WHERE country IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN product_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS product_id_tier,
    PERCENTILE_APPROX(qty, 0.9) AS qty_p90,
    APPROX_PERCENTILE(customer_id_sum_product_id, 0.5) AS customer_id_sum_product_id_p50,
    COUNT(*) AS num_rows,
    GROUPING_ID() AS grouping_id
FROM cleaned
GROUP BY ROLLUP (CASE WHEN product_id >= 9 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY qty_p90
