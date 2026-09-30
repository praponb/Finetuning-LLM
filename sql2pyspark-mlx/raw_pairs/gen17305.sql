SELECT id, product_id, quantity, CAST(unit_price AS INT) AS unit_price_int, FLOOR(quantity) AS quantity_whole FROM order_items WHERE order_id = 1;
