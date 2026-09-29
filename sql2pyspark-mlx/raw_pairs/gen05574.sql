SELECT id, order_id, quantity FROM order_items WHERE order_id IN (4, 14, 39) INTERSECT SELECT id, order_id, quantity FROM order_items WHERE quantity != 7
