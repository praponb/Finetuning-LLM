SELECT status, 'payment' AS kind
FROM payments
UNION
SELECT status, 'shipment' AS kind
FROM shipments
WHERE status IN ('LOST', 'DELIVERED')
ORDER BY status;
