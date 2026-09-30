SELECT order_id, COALESCE(quantity, 0) AS quantity_filled, quantity - unit_price AS line_total
FROM order_items
