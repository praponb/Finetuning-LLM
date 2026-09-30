SELECT country, city
FROM suppliers
INTERSECT
SELECT country, city
FROM suppliers
WHERE city IN ('New York', 'Tokyo');
