SELECT country
FROM suppliers
WHERE country = 'JP'
INTERSECT
SELECT country
FROM suppliers
WHERE rating < 2.0
ORDER BY country;
