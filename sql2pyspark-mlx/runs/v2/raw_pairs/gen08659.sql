WITH recent_products AS (
    SELECT *
    FROM products
    WHERE cost IS NOT NULL AND brand IS NOT NULL AND LOWER(TRIM(product_name)) = 'office chair'
),
products_stats AS (
    SELECT brand, category, COUNT(DISTINCT price) AS unique_categorys
    FROM recent_products
    GROUP BY brand, category
)
SELECT *
FROM products_stats
ORDER BY brand ASC
