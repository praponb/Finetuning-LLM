SELECT city
FROM customers
EXCEPT DISTINCT
SELECT city
FROM customers
WHERE is_active;
