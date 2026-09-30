SELECT p.id, p.price, p.cost, p.launch_date FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id IN (10, 30)) ORDER BY 3 DESC, id LIMIT 50
