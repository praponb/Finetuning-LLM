SELECT city
FROM suppliers
UNION ALL
SELECT city
FROM stores
WHERE store_name LIKE 'A%'
