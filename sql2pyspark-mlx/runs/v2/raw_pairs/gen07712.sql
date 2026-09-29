SELECT status, 'payment' AS source
FROM payments
UNION
SELECT status, 'shipment' AS source
FROM shipments
ORDER BY source
