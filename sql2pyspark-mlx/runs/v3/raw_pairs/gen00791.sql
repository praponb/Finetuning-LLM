SELECT p.id, p.stock_qty, p.product_name FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id) ORDER BY stock_qty DESC;
