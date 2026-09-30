SELECT id, email
FROM customers
WHERE city = 'Bangkok'
UNION
SELECT id, email
FROM customers
WHERE age < 42
