SELECT id, order_id, quantity FROM (SELECT id, order_id, quantity, RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS pos FROM order_items) AS t WHERE pos <= 3
