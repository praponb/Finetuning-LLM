SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, SUM(price) AS total_price, RANK() OVER (ORDER BY SUM(price) DESC) AS price_rank
FROM products
GROUP BY DATE_TRUNC('MONTH', launch_date)
ORDER BY total_price DESC
