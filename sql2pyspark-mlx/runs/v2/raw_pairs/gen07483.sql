SELECT id, product_id, quantity, LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY quantity, id) AS next_quantity FROM order_items ORDER BY id DESC, product_id
