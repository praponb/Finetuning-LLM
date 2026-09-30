SELECT email
FROM customers
WHERE city = 'New York'
UNION ALL
SELECT email
FROM customers
WHERE city IN ('Tokyo', 'Chiang Mai', 'Singapore')
