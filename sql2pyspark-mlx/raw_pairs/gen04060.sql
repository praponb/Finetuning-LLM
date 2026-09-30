SELECT id, order_id, quantity, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS quantity_dense_rank FROM order_items WHERE unit_price > 120
