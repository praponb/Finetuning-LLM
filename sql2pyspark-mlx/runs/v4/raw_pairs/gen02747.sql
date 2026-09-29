SELECT p.id, p.product_name, p.stock_qty, p.brand FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.product_id IN (28, 36));
