SELECT id, status, order_date
FROM orders
WHERE store_id IS NULL
UNION ALL
SELECT id, status, order_date
FROM orders
WHERE store_id IS NULL
ORDER BY status
