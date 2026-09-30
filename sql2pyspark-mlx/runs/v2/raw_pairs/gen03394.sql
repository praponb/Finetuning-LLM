SELECT order_date, channel
FROM orders
WHERE order_date >= '2025-01-05'
EXCEPT
SELECT order_date, channel
FROM orders
WHERE store_id IN (4, 18, 21)
ORDER BY channel
