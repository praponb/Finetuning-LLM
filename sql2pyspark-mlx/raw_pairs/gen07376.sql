SELECT id, order_id, quantity, quantity + unit_price AS line_total FROM order_items ORDER BY id DESC
