SELECT status, 'account' AS source
FROM accounts
UNION ALL
SELECT status, 'shipment' AS source
FROM shipments;
