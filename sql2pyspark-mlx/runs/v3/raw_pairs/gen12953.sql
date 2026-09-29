SELECT oi.id, oi.quantity, oi.product_id, p.supplier_id, p.product_name, p.launch_date FROM order_items oi INNER JOIN products p ON p.id = oi.product_id ORDER BY launch_date
