SELECT city, 'supplier' AS source
FROM suppliers
WHERE city = 'Bangkok'
UNION
SELECT city, 'store' AS source
FROM stores
