SELECT 'shipment' AS source, shipped_date
FROM shipments
UNION
SELECT 'account' AS source, opened_date
FROM accounts
ORDER BY source;
