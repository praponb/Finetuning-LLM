SELECT country
FROM customers
INTERSECT
SELECT country
FROM sessions;
