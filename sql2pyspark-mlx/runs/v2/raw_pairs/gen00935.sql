SELECT id, product_id, quantity, RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS quantity_rank FROM order_items;
