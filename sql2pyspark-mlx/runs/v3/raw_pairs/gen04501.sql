SELECT country
FROM customers
WHERE LOWER(name) LIKE '%mma%'
UNION ALL
SELECT country
FROM sessions
ORDER BY country DESC;
