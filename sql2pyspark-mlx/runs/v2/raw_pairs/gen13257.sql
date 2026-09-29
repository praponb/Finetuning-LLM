SELECT city
FROM suppliers
UNION
SELECT city
FROM stores
ORDER BY city DESC
