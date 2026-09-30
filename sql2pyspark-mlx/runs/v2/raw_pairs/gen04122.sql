SELECT quantity, order_id, unit_price * quantity AS line_total, unit_price + quantity AS line_total_2 FROM order_items WHERE quantity = 8 AND unit_price BETWEEN 200 AND 240
