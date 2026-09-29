SELECT id, quantity, product_id FROM order_items WHERE quantity > (SELECT AVG(quantity) FROM order_items WHERE order_id IN (25, 31, 33)) ORDER BY 3 ASC
