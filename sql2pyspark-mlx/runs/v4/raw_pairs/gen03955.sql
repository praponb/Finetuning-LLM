SELECT store_id
FROM orders
WHERE status NOT IN ('PENDING', 'SHIPPED')
UNION ALL
SELECT store_id
FROM orders
WHERE channel IN ('mobile', 'store', 'partner');
