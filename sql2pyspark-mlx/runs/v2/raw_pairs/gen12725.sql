SELECT status, 'payment' source
FROM payments
UNION
SELECT status, 'shipment' source
FROM shipments
ORDER BY status;
