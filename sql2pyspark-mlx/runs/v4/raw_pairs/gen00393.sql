SELECT * FROM (SELECT order_id, product_id, COUNT(*) AS row_count FROM order_items WHERE order_id IN (23, 27, 36) GROUP BY order_id, product_id) AS s WHERE row_count >= 5;
