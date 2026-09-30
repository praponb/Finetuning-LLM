SELECT id, quantity, product_id, unit_price FROM order_items WHERE product_id IS NULL AND order_id IN (37, 38);
