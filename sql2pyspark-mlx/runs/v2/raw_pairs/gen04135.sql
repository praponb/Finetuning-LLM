SELECT country, 'customer' AS source
FROM customers
WHERE age IS NOT NULL
UNION
SELECT country, 'session' AS source
FROM sessions
ORDER BY country DESC;
