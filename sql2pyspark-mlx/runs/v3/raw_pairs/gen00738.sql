SELECT order_id, CEIL(quantity) AS quantity_whole FROM order_items WHERE quantity < 6;
