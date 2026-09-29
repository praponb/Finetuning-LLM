SELECT status, 'account' AS source
FROM accounts
UNION ALL
SELECT status, 'shipment' AS source
FROM shipments
WHERE order_id IN (1, 7, 13)
