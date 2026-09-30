SELECT oi.id, oi.product_id FROM order_items oi WHERE oi.product_id IS NOT NULL AND oi.order_id IN (SELECT id FROM orders WHERE customer_id IN (10, 30, 32, 40));
