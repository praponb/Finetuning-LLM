SELECT id, quantity, product_id, order_id FROM order_items WHERE quantity > (SELECT MAX(quantity) FROM order_items) ORDER BY product_id
