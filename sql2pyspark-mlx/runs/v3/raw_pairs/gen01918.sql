SELECT country, 'customer' AS source
FROM customers
UNION ALL
SELECT country, 'session' AS source
FROM sessions
ORDER BY source DESC;
