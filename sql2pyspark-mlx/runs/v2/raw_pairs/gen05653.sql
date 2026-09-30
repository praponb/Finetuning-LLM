SELECT status, 'account' source
FROM accounts
UNION
SELECT status, 'shipment' source
FROM shipments
