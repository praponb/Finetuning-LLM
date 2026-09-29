SELECT id, product_id, order_id FROM order_items WHERE product_id IS NOT NULL UNION SELECT id, product_id, order_id FROM order_items WHERE quantity BETWEEN 3 AND 5;
