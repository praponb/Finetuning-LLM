SELECT store_id, status
FROM orders
WHERE order_date BETWEEN '2024-06-05' AND '2025-04-01'
UNION
SELECT store_id, status
FROM orders
WHERE store_id IN (1, 15, 26)
