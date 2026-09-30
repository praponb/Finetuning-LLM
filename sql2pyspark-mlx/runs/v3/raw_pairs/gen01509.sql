SELECT id, product_id, order_id, quantity, LEAD(quantity, 1, 0) OVER (PARTITION BY product_id, order_id ORDER BY quantity, id) next_quantity FROM order_items
