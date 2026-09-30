SELECT id, quantity, product_id FROM order_items WHERE quantity >= (SELECT AVG(quantity) FROM order_items WHERE product_id = 36) ORDER BY quantity DESC, id LIMIT 10;
