SELECT id, unit_price, order_id, product_id FROM order_items WHERE product_id IN (16, 24, 31) AND (order_id = 5 OR quantity BETWEEN 6 AND 7);
