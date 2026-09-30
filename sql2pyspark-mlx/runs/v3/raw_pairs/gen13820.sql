SELECT launch_date
FROM products
WHERE ARRAY_CONTAINS(tags, 'bestseller')
UNION
SELECT delivered_date launch_date
FROM shipments
WHERE status = 'DELIVERED';
