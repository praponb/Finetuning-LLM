SELECT id, unit_price, unit_price + quantity AS line_total FROM order_items WHERE product_id IS NOT NULL AND unit_price > 230 ORDER BY line_total DESC, id LIMIT 50;
