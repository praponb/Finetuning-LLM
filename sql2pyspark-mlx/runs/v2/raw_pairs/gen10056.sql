SELECT status, 'payment' AS source
FROM payments
WHERE status = 'SUCCESS'
UNION ALL
SELECT status, 'shipment' AS source
FROM shipments
