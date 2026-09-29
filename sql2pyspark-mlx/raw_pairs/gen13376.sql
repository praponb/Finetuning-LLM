SELECT o.id, o.status, o.customer_id
FROM orders o
WHERE o.store_id IN (SELECT id FROM stores WHERE region = 'North');
