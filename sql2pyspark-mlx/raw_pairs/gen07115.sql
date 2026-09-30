SELECT id, city
FROM customers
EXCEPT DISTINCT
SELECT id, city
FROM customers
WHERE LOWER(TRIM(name)) = 'emma brown'
