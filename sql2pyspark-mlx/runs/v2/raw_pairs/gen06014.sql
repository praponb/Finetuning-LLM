SELECT product_id FROM order_items WHERE unit_price IS NULL UNION SELECT product_id FROM order_items WHERE unit_price IS NOT NULL ORDER BY product_id;
