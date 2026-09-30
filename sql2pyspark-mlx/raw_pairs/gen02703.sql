SELECT p.id, p.category, p.cost FROM products p WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.product_id IN (5, 25, 26, 28));
