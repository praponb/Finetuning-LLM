WITH filtered_products AS (
    SELECT *
    FROM products
    WHERE (launch_date BETWEEN '2023-06-25' AND '2024-10-01' AND launch_date < '2023-06-20') OR stock_qty BETWEEN 142 AND 221
),
products_stats AS (
    SELECT brand, MIN(launch_date) AS first_launch_date
    FROM filtered_products
    GROUP BY brand
)
SELECT *
FROM products_stats
ORDER BY 1 DESC;
