WITH recent_products AS (
    SELECT *
    FROM products
    WHERE product_name LIKE 'R%'
),
category_summary AS (
    SELECT
        category,
        brand,
        MAX(price) - MIN(price) AS price_range,
        ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM recent_products
    GROUP BY category, brand
)
SELECT *
FROM category_summary
WHERE price_range > 27
ORDER BY category DESC, brand
LIMIT 10
