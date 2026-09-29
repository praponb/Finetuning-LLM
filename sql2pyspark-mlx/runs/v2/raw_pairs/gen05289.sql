SELECT segment
FROM customers
WHERE LENGTH(name) >= 13
INTERSECT
SELECT segment
FROM customers
WHERE age < 50
