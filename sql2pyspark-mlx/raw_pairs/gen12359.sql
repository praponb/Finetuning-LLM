SELECT 'account' AS origin, status
FROM accounts
UNION
SELECT 'shipment' AS origin, status
FROM shipments
WHERE shipped_date BETWEEN '2024-01-29' AND '2024-12-01'
ORDER BY status
