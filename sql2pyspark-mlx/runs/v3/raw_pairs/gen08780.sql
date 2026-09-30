SELECT quantity FROM order_items WHERE product_id IS NULL UNION SELECT quantity FROM order_items WHERE product_id IS NOT NULL;
