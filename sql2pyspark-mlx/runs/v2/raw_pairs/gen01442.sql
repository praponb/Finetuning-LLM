SELECT id, order_id, quantity, NTILE(10) OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS bucket FROM order_items WHERE quantity <= 6
