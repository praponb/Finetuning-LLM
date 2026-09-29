SELECT id, product_id, unit_price, quantity, FLOOR(quantity) AS quantity_whole FROM order_items WHERE order_id IN (4, 17, 34) ORDER BY product_id DESC;
