SELECT country
FROM suppliers
WHERE rating <= 3.1
UNION ALL
SELECT country
FROM sessions
ORDER BY country
