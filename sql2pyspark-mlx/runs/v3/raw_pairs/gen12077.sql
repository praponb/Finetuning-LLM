SELECT country, 'customer' AS source
FROM customers
UNION
SELECT country, 'session' AS source
FROM sessions
WHERE browser != 'Safari'
