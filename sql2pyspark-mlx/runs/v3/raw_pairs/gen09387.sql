SELECT city
FROM suppliers
WHERE rating <= 3.5
UNION ALL
SELECT city
FROM stores
ORDER BY city
