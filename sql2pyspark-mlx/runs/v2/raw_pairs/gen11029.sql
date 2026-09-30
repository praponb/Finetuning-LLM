SELECT p.id, p.brand, p.supplier_id, p.category FROM products p WHERE p.price <= 890 AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id IN (7, 32));
