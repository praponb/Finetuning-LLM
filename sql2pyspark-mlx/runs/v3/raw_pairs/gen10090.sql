SELECT p.id, p.product_name FROM products p WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.quantity < 7)
