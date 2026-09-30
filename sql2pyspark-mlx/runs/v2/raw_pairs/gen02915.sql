SELECT status, 'account' AS source
FROM accounts
UNION ALL
SELECT status, 'shipment' AS source
FROM shipments
WHERE delivered_date BETWEEN '2024-06-08' AND '2025-07-01'
