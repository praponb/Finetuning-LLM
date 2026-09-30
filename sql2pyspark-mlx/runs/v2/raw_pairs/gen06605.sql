SELECT oi.id, oi.quantity, p.supplier_id, p.launch_date, p.category FROM order_items oi INNER JOIN products p ON oi.product_id = p.id ORDER BY quantity DESC;
