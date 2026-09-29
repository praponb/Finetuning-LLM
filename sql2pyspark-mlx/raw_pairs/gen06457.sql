SELECT DATE_TRUNC('MONTH', order_date) AS order_month, AVG(discount) AS avg_discount, SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
FROM orders
GROUP BY DATE_TRUNC('MONTH', order_date)
ORDER BY order_month
