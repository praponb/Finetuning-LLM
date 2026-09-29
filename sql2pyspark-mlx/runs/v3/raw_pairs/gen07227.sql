SELECT id, product_id, quantity, unit_price, unit_price + quantity AS line_total, ROUND(unit_price / quantity, 2) AS line_total_2 FROM order_items WHERE order_id IN (19, 27, 35, 37)
