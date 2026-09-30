SELECT id, product_id, unit_price, MIN(unit_price) OVER (PARTITION BY product_id) AS product_id_min_unit_price FROM order_items WHERE order_id IN (8, 10, 24, 28);
