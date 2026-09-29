SELECT quantity FROM order_items WHERE product_id IS NOT NULL INTERSECT SELECT quantity FROM order_items WHERE quantity > 5;
