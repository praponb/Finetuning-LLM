SELECT oi.id, oi.order_id, oi.unit_price, oi.quantity, p.supplier_id FROM order_items oi JOIN products p ON oi.product_id = p.id ORDER BY oi.quantity;
