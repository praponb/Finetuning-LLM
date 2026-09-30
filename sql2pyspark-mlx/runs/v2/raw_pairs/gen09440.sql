SELECT id, quantity, product_id, unit_price FROM order_items WHERE quantity < (SELECT MIN(quantity) FROM order_items WHERE order_id IN (4, 13, 23, 35))
