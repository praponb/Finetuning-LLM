SELECT oi.id, oi.quantity, o.channel FROM order_items oi LEFT JOIN orders o ON o.id = oi.order_id WHERE o.order_date <= '2023-08-21' ORDER BY quantity DESC, id LIMIT 10
