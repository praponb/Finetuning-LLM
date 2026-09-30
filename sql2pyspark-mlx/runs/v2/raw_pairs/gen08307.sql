SELECT id, quantity, order_id FROM order_items WHERE unit_price < 260 UNION ALL SELECT id, quantity, order_id FROM order_items WHERE unit_price >= 140 ORDER BY id;
