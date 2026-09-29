SELECT order_date, channel
FROM orders
WHERE store_id IN (4, 25, 28, 33)
UNION
SELECT order_date, channel
FROM orders
WHERE status = 'COMPLETED'
ORDER BY channel;
