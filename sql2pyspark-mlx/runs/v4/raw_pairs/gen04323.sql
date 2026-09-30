SELECT id, order_id FROM order_items WHERE unit_price >= 120 INTERSECT SELECT id, order_id FROM order_items WHERE product_id IN (4, 12, 26, 36) ORDER BY id;
