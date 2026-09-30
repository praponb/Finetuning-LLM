SELECT id, unit_price, quantity, product_id FROM order_items WHERE NOT (order_id = 2 OR order_id = 14)
