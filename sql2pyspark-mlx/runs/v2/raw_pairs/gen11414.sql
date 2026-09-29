SELECT
    DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month,
    MIN(launch_date) AS first_launch_date,
    MAX(price) - MIN(price) AS price_range,
    MIN(launch_date) AS min_launch_date
FROM products
GROUP BY launch_month
HAVING MIN(stock_qty) > 200
ORDER BY launch_month
LIMIT 5;
