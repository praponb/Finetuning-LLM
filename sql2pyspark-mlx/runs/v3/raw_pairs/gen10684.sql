SELECT o.id, o.customer_id, o.amount
FROM orders o
WHERE o.store_id NOT IN (SELECT id FROM stores WHERE city IN ('Singapore', 'Bangkok'));
