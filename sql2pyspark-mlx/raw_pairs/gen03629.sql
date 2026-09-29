SELECT id, order_id, unit_price, MIN(unit_price) OVER (PARTITION BY order_id) AS order_id_min_unit_price FROM order_items
