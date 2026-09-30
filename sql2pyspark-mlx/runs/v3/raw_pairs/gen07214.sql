SELECT status
FROM orders
WHERE status NOT IN ('COMPLETED', 'CANCELLED')
UNION
SELECT status
FROM shipments
WHERE carrier <> 'FedEx'
ORDER BY status DESC
