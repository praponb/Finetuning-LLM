SELECT DATE_TRUNC('MONTH', order_date) AS order_month, COUNT(*) AS total_count, SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END) AS shipped_count
FROM orders
GROUP BY DATE_TRUNC('MONTH', order_date)
ORDER BY order_month
LIMIT 50;
