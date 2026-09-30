SELECT 'payment' AS source, status
FROM payments
UNION
SELECT 'shipment' AS source, status
FROM shipments
ORDER BY status
