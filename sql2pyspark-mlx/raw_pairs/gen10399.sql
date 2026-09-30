SELECT id, order_id, quantity, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS row_num FROM order_items WHERE quantity BETWEEN 5 AND 8
