SELECT quantity, product_id FROM order_items WHERE order_id IN (24, 34, 39) INTERSECT SELECT quantity, product_id FROM order_items WHERE unit_price < 320 ORDER BY quantity DESC;
