WITH base AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        ROUND(cost * 0.9, 2) AS cost_scaled,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-05-08'
      AND brand IS NOT NULL
)
SELECT
    id,
    category,
    price,
    cost_scaled,
    brand_filled,
    NTILE(10) OVER (PARTITION BY brand, category ORDER BY price, id) AS price_bucket,
    LAG(cost_scaled, 1) OVER (PARTITION BY category, brand ORDER BY price, id) AS prev_cost_scaled,
    NTILE(3) OVER (PARTITION BY brand ORDER BY cost_scaled, id) AS cost_scaled_bucket,
    PERCENT_RANK() OVER (PARTITION BY MONTH(launch_date), category ORDER BY price DESC) AS price_percent_rank,
    LAST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_brand
FROM base
