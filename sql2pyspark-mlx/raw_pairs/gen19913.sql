SELECT oi.id, oi.quantity, oi.order_id, o.amount, s.region, s.sqft FROM order_items oi JOIN orders o ON o.id = oi.order_id LEFT JOIN stores s ON o.store_id = s.id;
