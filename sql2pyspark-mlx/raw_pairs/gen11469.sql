SELECT id, order_id, quantity, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) rn FROM order_items;
