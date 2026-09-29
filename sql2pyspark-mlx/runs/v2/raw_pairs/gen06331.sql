SELECT status
FROM orders
WHERE channel = 'store'
UNION
SELECT status
FROM payments
ORDER BY status DESC
