SELECT p.id, p.category, p.launch_date, p.product_name FROM products p WHERE p.supplier_id IN (7, 8, 34, 35) AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id);
