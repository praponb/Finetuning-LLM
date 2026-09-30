SELECT method
FROM payments
INTERSECT
SELECT status
FROM shipments
WHERE order_id IS NULL
ORDER BY method
