SELECT id, quantity, ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS rn FROM order_items
