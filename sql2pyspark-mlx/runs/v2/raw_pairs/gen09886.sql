SELECT *
FROM orders
WHERE store_id IS NOT NULL AND status = 'SHIPPED'
