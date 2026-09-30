SELECT city
FROM customers
EXCEPT
SELECT city
FROM suppliers
WHERE country = 'US'
