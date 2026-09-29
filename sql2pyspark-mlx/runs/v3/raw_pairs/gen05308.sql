SELECT oi.id, oi.product_id, oi.quantity, oi.order_id FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE product_name IS NOT NULL)
