SELECT channel, DATE_TRUNC('MONTH', order_date) AS order_month, COUNT(*) AS cnt
FROM orders
WHERE order_date < '2023-12-06'
GROUP BY channel, DATE_TRUNC('MONTH', order_date)
HAVING COUNT(*) > 2
