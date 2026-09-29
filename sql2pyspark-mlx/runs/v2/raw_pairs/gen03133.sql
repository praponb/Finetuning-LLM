SELECT id, order_id FROM order_items WHERE unit_price >= 130 UNION ALL SELECT id, order_id FROM order_items WHERE quantity >= 5 ORDER BY order_id DESC;
