SELECT oi.id, oi.product_id FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE customer_id IN (18, 20));
