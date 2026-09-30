SELECT country, 'supplier' AS source
FROM suppliers
UNION
SELECT country, 'session' AS source
FROM sessions
ORDER BY source DESC
