SELECT id, name
FROM customers
WHERE segment = 'SMB'
UNION
SELECT id, name
FROM customers
WHERE city IN ('London', 'Bangkok')
