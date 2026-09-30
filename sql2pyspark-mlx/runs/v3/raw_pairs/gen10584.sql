SELECT id, order_id, quantity FROM order_items WHERE order_id IN (12, 21, 32) UNION ALL SELECT id, order_id, quantity FROM order_items WHERE product_id = 5 ORDER BY quantity
