SELECT id, order_id, unit_price, MAX(unit_price) OVER (PARTITION BY order_id) AS order_id_max_unit_price FROM order_items;
