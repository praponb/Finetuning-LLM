SELECT id, order_id, quantity, RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS quantity_rank FROM order_items;
