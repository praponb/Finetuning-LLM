WITH filtered_products AS (
    SELECT *
    FROM products
    WHERE product_name LIKE 'Y%' OR brand != 'Initech'
),
summary AS (
    SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, MIN(launch_date) AS first_launch_date
    FROM filtered_products
    GROUP BY DATE_TRUNC('MONTH', launch_date)
)
SELECT launch_month, first_launch_date
FROM summary
ORDER BY first_launch_date ASC;
