SELECT oi.product_id, oi.order_id, p.supplier_id FROM order_items oi LEFT JOIN products p ON p.id = oi.product_id WHERE p.product_name NOT LIKE '%Noise%';
