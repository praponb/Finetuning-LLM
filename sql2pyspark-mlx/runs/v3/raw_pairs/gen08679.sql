SELECT id, order_id FROM order_items WHERE unit_price >= 120 AND product_id IN (13, 30, 31, 33) AND quantity <= 5 ORDER BY id;
