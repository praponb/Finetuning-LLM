SELECT city, 'supplier' source
FROM suppliers
WHERE city IN ('New York', 'London')
UNION ALL
SELECT city, 'store' source
FROM stores
WHERE city <> 'Bangkok';
