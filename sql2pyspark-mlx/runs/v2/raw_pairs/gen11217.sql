SELECT product_id FROM order_items WHERE quantity <= 4 UNION ALL SELECT product_id FROM order_items WHERE quantity > 5 ORDER BY product_id DESC;
