SELECT status, 'payment' AS source
FROM payments
WHERE amount > 1490
UNION ALL
SELECT status, 'shipment' AS source
FROM shipments
ORDER BY status;
