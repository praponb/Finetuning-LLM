SELECT * FROM order_items WHERE product_id = 40 AND (product_id IN (10, 23, 26, 37) OR order_id IN (12, 13, 24));
