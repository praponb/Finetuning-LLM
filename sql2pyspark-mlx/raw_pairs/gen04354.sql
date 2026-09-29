SELECT oi.order_id, oi.quantity, p.supplier_id FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id ORDER BY oi.quantity DESC
