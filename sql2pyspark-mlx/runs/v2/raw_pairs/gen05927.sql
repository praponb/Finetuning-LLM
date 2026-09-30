SELECT country, 'customer' AS source
FROM customers
WHERE country IN ('US', 'DE')
UNION
SELECT country, 'session' AS source
FROM sessions
ORDER BY source;
