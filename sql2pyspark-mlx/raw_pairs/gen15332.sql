SELECT product_id, order_id, unit_price * quantity AS line_total FROM order_items WHERE unit_price >= 230 AND quantity > 4 AND product_id = 29
