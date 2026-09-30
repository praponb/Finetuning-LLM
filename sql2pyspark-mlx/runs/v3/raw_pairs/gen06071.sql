SELECT country
FROM suppliers
WHERE city = 'New York'
UNION ALL
SELECT country
FROM sessions
ORDER BY country;
