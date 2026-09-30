SELECT id, quantity, unit_price, order_id FROM order_items WHERE quantity >= (SELECT MAX(quantity) FROM order_items WHERE unit_price IS NOT NULL);
