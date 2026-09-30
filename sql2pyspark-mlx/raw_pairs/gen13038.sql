SELECT p.id, p.category, p.launch_date FROM products p WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.product_id = 34);
