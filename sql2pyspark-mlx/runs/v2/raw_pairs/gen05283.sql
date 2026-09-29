SELECT p.id, p.supplier_id, p.launch_date, p.product_name FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id);
