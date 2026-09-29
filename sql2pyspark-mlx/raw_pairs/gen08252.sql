SELECT p.id, p.category, p.launch_date FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.quantity < 3)
