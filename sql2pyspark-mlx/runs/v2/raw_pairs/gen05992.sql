SELECT city, 'supplier' source
FROM suppliers
UNION ALL
SELECT city, 'store' source
FROM stores
ORDER BY source DESC
