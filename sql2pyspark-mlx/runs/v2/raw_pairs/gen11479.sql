SELECT status
FROM payments
UNION
SELECT status
FROM shipments
ORDER BY status DESC
