SELECT oi.unit_price, oi.quantity, o.order_date, o.channel FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.id WHERE oi.quantity < 8
