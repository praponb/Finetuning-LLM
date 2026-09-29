SELECT product_id, unit_price * quantity AS line_total FROM order_items WHERE unit_price >= 180 ORDER BY product_id ASC;
