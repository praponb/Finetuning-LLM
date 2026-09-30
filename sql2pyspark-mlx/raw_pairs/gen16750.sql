SELECT id, status, order_date
FROM orders
WHERE store_id = 22
UNION ALL
SELECT id, status, order_date
FROM orders
WHERE amount > 420
ORDER BY id DESC
