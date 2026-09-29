SELECT status, 'account' AS source
FROM accounts
UNION
SELECT status, 'shipment' AS source
FROM shipments
ORDER BY status;
