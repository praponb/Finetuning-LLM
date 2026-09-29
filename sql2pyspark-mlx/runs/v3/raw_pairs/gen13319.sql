SELECT unit_price FROM order_items UNION ALL SELECT cost FROM shipments WHERE order_id IN (10, 11, 30);
