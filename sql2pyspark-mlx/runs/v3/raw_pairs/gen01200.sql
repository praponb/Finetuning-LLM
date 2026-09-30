SELECT country, 'supplier' AS source
FROM suppliers
UNION ALL
SELECT country, 'session' AS source
FROM sessions
WHERE browser = 'Edge';
