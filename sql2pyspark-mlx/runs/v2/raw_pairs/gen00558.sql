SELECT country
FROM suppliers
UNION ALL
SELECT country
FROM sessions
ORDER BY country;
