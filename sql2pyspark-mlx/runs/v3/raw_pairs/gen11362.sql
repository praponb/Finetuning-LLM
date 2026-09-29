SELECT id, product_id, quantity FROM order_items WHERE product_id IN (9, 27, 28, 39) INTERSECT SELECT id, product_id, quantity FROM order_items WHERE unit_price BETWEEN 150 AND 300;
