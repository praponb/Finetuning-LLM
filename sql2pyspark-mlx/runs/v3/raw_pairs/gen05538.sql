SELECT carrier
FROM shipments
WHERE status = 'DELIVERED'
UNION ALL
SELECT carrier
FROM shipments
WHERE delivered_date > '2024-04-01'
ORDER BY carrier DESC;
