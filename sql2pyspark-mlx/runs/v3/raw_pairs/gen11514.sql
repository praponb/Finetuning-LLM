SELECT country, city
FROM suppliers
WHERE city NOT IN ('New York', 'London')
EXCEPT
SELECT country, city
FROM suppliers
WHERE city NOT IN ('New York', 'London', 'Berlin')
