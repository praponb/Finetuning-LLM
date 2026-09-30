SELECT id, quantity FROM order_items WHERE quantity BETWEEN 5 AND 6 EXCEPT SELECT id, quantity FROM order_items WHERE order_id IN (19, 21, 37)
