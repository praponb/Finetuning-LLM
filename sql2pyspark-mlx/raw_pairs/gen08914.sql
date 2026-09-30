SELECT unit_price FROM order_items WHERE (order_id IN (3, 5, 26, 32) AND order_id = 32) OR product_id IN (15, 19, 31);
