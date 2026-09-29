SELECT id, quantity, unit_price, order_id FROM order_items WHERE order_id IN (15, 16, 19, 29) AND quantity >= 7 AND product_id IN (15, 22, 29, 31) ORDER BY order_id DESC, id LIMIT 1;
