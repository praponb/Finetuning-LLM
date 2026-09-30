SELECT id, order_id, quantity FROM order_items WHERE quantity > 3 INTERSECT SELECT id, order_id, quantity FROM order_items WHERE unit_price >= 330
