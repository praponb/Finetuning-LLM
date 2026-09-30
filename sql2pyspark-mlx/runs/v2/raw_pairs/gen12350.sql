SELECT p.id, p.launch_date, p.supplier_id, p.brand FROM products p WHERE SIZE(p.tags) > 2 AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id);
