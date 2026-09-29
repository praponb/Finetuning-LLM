SELECT id, product_id, quantity, NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_ntile FROM order_items WHERE quantity BETWEEN 6 AND 7;
