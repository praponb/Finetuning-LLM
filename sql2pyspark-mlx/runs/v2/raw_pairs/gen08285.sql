SELECT id, name
FROM customers
WHERE country = 'DE'
UNION ALL
SELECT id, name
FROM customers
WHERE city IS NOT NULL
ORDER BY name DESC
