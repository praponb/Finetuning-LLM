SELECT id, order_id, quantity, MAX(quantity) OVER (PARTITION BY order_id) AS order_id_max_quantity FROM order_items ORDER BY order_id DESC
