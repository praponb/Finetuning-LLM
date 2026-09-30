SELECT country, 'customer' AS source
FROM customers
UNION ALL
SELECT country, 'session' AS source
FROM sessions
WHERE country IN ('TH', 'JP')
ORDER BY country
