SELECT MONTH(order_date) AS order_month, COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count, MAX(order_date) AS latest_order_date
FROM orders
WHERE order_date BETWEEN '2023-11-10' AND '2025-01-18'
GROUP BY MONTH(order_date)
HAVING COUNT(CASE WHEN channel = 'store' THEN 1 END) >= 3
ORDER BY latest_order_date, order_month
LIMIT 20
