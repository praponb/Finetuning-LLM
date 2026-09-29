SELECT DATE_TRUNC('MONTH', order_date) AS order_month, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY DATE_TRUNC('MONTH', order_date)) AS cumulative_amount
FROM orders
GROUP BY DATE_TRUNC('MONTH', order_date)
ORDER BY total_amount DESC;
