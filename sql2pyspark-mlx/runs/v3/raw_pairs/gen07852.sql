SELECT oi.id, oi.quantity, oi.product_id FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE customer_id IS NOT NULL) ORDER BY id LIMIT 5
