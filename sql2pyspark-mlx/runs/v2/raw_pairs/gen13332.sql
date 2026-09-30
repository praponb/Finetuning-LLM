SELECT oi.id, oi.product_id, oi.quantity, p.supplier_id, p.product_name FROM order_items oi LEFT OUTER JOIN products p ON oi.product_id = p.id ORDER BY p.product_name;
