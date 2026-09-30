SELECT status, 'account' AS source
FROM accounts
UNION
SELECT status, 'shipment' AS source
FROM shipments
WHERE delivered_date > '2023-07-01'
ORDER BY status DESC;
