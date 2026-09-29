SELECT product_id, unit_price, order_id, COALESCE(unit_price, 0) AS unit_price_filled, unit_price * quantity AS line_total FROM order_items WHERE unit_price <= 380 ORDER BY order_id, product_id DESC;
