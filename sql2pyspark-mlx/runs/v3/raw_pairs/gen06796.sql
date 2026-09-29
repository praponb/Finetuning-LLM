WITH ValidProducts AS (
    SELECT *
    FROM products
    WHERE brand IS NULL AND category = 'Electronics'
),
category_summary AS (
    SELECT category, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
    FROM ValidProducts
    GROUP BY category
)
SELECT *
FROM category_summary
ORDER BY brand_values, category
LIMIT 1
