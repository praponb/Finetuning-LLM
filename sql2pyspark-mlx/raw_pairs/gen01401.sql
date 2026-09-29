SELECT status, 'account' AS kind
FROM accounts
UNION
SELECT status, 'shipment' AS kind
FROM shipments
ORDER BY kind;
