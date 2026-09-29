WITH src AS (
    SELECT *
    FROM products
    WHERE price <= 670 AND ARRAY_CONTAINS(tags, 'sale')
),
products_stats AS (
    SELECT category, COUNT(price) AS price_count
    FROM src
    GROUP BY category
)
SELECT category, price_count
FROM products_stats
ORDER BY price_count DESC;
