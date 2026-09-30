WITH filtered_products AS (
    SELECT *, LENGTH(category) AS category_length
    FROM products
    WHERE brand IS NULL AND brand IN ('Initech', 'Stark', 'Globex')
),
products_stats AS (
    SELECT category, MAX(cost) AS max_cost, MIN(launch_date) AS min_launch_date
    FROM filtered_products
    GROUP BY category
)
SELECT *
FROM products_stats
ORDER BY max_cost DESC;
