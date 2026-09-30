SELECT carrier
FROM shipments
WHERE delivered_date <= '2024-06-05'
UNION
SELECT carrier
FROM shipments
WHERE carrier = 'DHL'
ORDER BY carrier DESC
