SELECT order_id, unit_price * quantity AS line_total FROM order_items ORDER BY line_total DESC, order_id DESC
