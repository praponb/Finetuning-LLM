SELECT o.id, o.amount, o.customer_id
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.order_id IN (14, 15));
