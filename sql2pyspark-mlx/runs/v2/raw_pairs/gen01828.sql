SELECT MONTH(order_date) AS order_month, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY MONTH(order_date)) AS cumulative_amount
FROM orders
GROUP BY MONTH(order_date);
