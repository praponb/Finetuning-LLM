SELECT product_id, quantity, order_id, CEIL(quantity) quantity_whole FROM order_items WHERE NOT (order_id IN (15, 20) AND product_id IN (20, 27, 35));
