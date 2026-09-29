SELECT status
FROM orders
WHERE discount >= 0.29
UNION
SELECT status
FROM payments
WHERE order_id IS NULL
