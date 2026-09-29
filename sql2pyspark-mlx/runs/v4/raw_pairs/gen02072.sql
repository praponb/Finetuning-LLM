SELECT name, segment
FROM customers
WHERE UPPER(name) LIKE '%ACK%'
UNION
SELECT name, segment
FROM customers
WHERE email LIKE '%@gmail.com'
ORDER BY name DESC
