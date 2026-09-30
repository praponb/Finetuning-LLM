SELECT city
FROM suppliers
EXCEPT
SELECT city
FROM stores
ORDER BY city;
