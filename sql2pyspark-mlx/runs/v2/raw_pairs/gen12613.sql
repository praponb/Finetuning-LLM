SELECT id, quantity FROM order_items WHERE order_id IN (5, 10) UNION SELECT id, quantity FROM order_items WHERE unit_price > 380 ORDER BY quantity
