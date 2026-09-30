SELECT p.id, p.product_name, p.category, p.cost FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id) ORDER BY p.id DESC, 4 DESC
