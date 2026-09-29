SELECT o.id, o.customer_id
FROM orders o
WHERE o.customer_id IS NULL AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id);
