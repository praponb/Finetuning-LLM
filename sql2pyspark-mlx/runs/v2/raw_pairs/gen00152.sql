SELECT id, segment
FROM customers
WHERE city IN ('London', 'Bangkok')
INTERSECT
SELECT id, segment
FROM customers
WHERE city = 'Chiang Mai'
ORDER BY id;
