SELECT country
FROM suppliers
INTERSECT
SELECT country
FROM sessions
ORDER BY country DESC;
