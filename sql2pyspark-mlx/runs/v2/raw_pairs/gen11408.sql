SELECT oi.id, oi.product_id, oi.quantity, oi.order_id, p.cost, p.price, p.supplier_id FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id ORDER BY p.supplier_id DESC, p.price;
