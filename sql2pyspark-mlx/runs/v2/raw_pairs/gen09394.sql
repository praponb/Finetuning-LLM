SELECT *, quantity - unit_price AS line_total, unit_price + quantity AS line_total_2 FROM order_items WHERE unit_price >= 260 AND (product_id = 40 OR order_id IN (8, 15, 17, 19))
