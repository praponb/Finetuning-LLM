SELECT quantity, product_id FROM order_items WHERE unit_price < 240 AND (order_id = 3 OR product_id IS NOT NULL);
