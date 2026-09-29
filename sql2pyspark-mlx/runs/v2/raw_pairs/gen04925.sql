SELECT status
FROM orders
WHERE store_id IS NULL
UNION
SELECT status
FROM payments
WHERE paid_at >= '2024-01-01'
