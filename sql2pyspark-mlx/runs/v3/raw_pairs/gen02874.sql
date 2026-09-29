SELECT oi.quantity, oi.unit_price, o.status, s.city FROM order_items oi RIGHT OUTER JOIN orders o ON oi.order_id = o.id LEFT OUTER JOIN stores s ON s.id = o.store_id ORDER BY quantity DESC
