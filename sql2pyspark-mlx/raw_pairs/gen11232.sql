SELECT status
FROM orders
EXCEPT
SELECT status
FROM payments
WHERE order_id IN (2, 4, 16, 30)
ORDER BY status DESC
