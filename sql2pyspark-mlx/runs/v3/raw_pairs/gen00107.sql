SELECT oi.id, oi.unit_price, oi.order_id, oi.quantity, p.brand FROM order_items oi INNER JOIN products p ON p.id = oi.product_id ORDER BY id;
