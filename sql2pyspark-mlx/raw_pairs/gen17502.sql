SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, SUM(price) AS total_price, SUM(SUM(price)) OVER (ORDER BY DATE_TRUNC('MONTH', launch_date)) AS cumulative_price
FROM products
GROUP BY DATE_TRUNC('MONTH', launch_date);
