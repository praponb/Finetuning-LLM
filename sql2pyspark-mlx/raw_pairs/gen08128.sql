SELECT id, product_id FROM order_items WHERE quantity > 6 UNION ALL SELECT id, product_id FROM order_items WHERE unit_price > 220 ORDER BY product_id;
