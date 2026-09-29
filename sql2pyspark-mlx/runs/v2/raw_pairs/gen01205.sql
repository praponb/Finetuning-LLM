WITH base AS (
    SELECT *
    FROM products
    WHERE product_name LIKE '%Shoe%'
),
products_stats AS (
    SELECT category, MAX(launch_date) AS latest_launch_date, MIN(launch_date) AS min_launch_date
    FROM base
    GROUP BY category
)
SELECT *
FROM products_stats
