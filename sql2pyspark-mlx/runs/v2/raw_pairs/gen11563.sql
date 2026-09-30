SELECT order_id, quantity FROM order_items WHERE order_id IN (5, 11, 17, 37) UNION ALL SELECT order_id, quantity FROM order_items WHERE unit_price >= 110
