SELECT store_id, status
FROM orders
WHERE amount IS NOT NULL
UNION
SELECT store_id, status
FROM orders
WHERE order_date > '2025-07-03'
