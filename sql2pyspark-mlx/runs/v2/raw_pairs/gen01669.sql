SELECT city, country
FROM customers
WHERE city = 'Tokyo'
INTERSECT
SELECT city, country
FROM suppliers
ORDER BY country DESC
