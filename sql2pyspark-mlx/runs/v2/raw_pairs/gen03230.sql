SELECT o.id, o.store_id, o.customer_id
FROM orders o
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price > 190)
