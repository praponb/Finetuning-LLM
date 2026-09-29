SELECT id, quantity, order_id, unit_price, CEIL(quantity) AS quantity_whole FROM order_items WHERE quantity BETWEEN 3 AND 4 ORDER BY quantity DESC, id LIMIT 1
