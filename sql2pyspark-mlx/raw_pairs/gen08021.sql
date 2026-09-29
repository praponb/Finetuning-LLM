SELECT oi.id, oi.order_id, oi.quantity FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE category = 'Fashion');
