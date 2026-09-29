WITH cleaned AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        launch_date,
        tags,
        supplier_id,
        ROUND(price * 100, 2) AS price_adjusted
    FROM products
    WHERE launch_date >= DATE '2023-07-19'
      AND supplier_id IS NOT NULL
      AND category IN ('Books', 'Home', 'Fashion')
)
SELECT
    CASE WHEN cost >= 700 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    COUNT_IF(brand = 'Umbrella') AS umbrella_events,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN category = 'Electronics' THEN 1 END), 0), 2) AS cost_per_electronics,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM cleaned
GROUP BY CASE WHEN cost >= 700 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN category = 'Electronics' THEN 1 END), 0), 2) >= 0
