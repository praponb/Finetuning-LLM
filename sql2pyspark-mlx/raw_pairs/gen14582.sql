SELECT country, id
FROM customers
INTERSECT
SELECT country, id
FROM customers
WHERE country = 'US';
