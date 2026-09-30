WITH top_products AS (
    SELECT
        id,
        category,
        launch_date,
        brand,
        RANK() OVER (PARTITION BY category ORDER BY launch_date DESC) AS rn
    FROM products
    WHERE SIZE(tags) > 1
)
SELECT
    id,
    category,
    launch_date,
    brand
FROM top_products
WHERE rn <= 3
