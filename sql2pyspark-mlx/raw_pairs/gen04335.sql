SELECT id, unit_price, product_id, unit_price - quantity AS line_total FROM order_items WHERE quantity >= 3 AND unit_price BETWEEN 240 AND 310 ORDER BY line_total, unit_price, id LIMIT 5;
