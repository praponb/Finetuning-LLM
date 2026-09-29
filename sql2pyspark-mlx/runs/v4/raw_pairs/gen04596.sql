SELECT id, product_id, order_id FROM order_items WHERE quantity = 8 INTERSECT SELECT id, product_id, order_id FROM order_items WHERE unit_price >= 190 ORDER BY id DESC
