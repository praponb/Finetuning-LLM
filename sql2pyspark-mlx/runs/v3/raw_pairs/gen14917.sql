SELECT country, 'supplier' AS source
FROM suppliers
WHERE country = 'UK'
UNION ALL
SELECT country, 'session' AS source
FROM sessions
ORDER BY country DESC;
