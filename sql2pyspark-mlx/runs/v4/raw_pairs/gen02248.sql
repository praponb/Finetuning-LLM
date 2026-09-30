SELECT city
FROM suppliers
WHERE rating <= 4.1
EXCEPT
SELECT city
FROM stores
ORDER BY city;
