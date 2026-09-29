SELECT unit_price, quantity, order_id, unit_price * quantity AS line_total FROM order_items WHERE product_id IN (9, 34)
