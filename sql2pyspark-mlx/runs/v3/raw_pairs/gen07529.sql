SELECT o.id, o.status, o.store_id
FROM orders o
WHERE o.customer_id = 31 AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)
ORDER BY status DESC
