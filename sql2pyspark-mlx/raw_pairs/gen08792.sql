SELECT 'supplier' AS source, city
FROM suppliers
WHERE rating BETWEEN 3.6 AND 3.8
UNION ALL
SELECT 'store' AS source, city
FROM stores
ORDER BY city;
