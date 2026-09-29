SELECT id, order_date
FROM orders
WHERE store_id IN (10, 27)
UNION ALL
SELECT id, order_date
FROM orders
WHERE status = 'PENDING'
ORDER BY id;
