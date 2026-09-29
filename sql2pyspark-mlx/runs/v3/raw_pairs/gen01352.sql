SELECT country
FROM suppliers
WHERE city IN ('Bangkok', 'London')
UNION ALL
SELECT country
FROM sessions
