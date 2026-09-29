SELECT id, order_id, quantity, SUM(quantity) OVER (PARTITION BY order_id) AS order_id_sum_quantity FROM order_items ORDER BY order_id_sum_quantity DESC
