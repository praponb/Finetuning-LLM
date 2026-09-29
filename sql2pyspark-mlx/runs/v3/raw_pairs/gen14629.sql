SELECT order_id FROM order_items WHERE unit_price > 220 UNION SELECT order_id FROM order_items WHERE quantity < 5
