SELECT id, quantity, order_id, unit_price FROM order_items WHERE order_id IN (1, 8, 12) AND (product_id = 7 OR quantity > 3) ORDER BY unit_price DESC, quantity DESC
