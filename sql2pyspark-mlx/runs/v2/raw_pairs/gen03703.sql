SELECT oi.order_id, oi.quantity, o.channel, o.order_date, p.cost, p.launch_date FROM order_items oi LEFT OUTER JOIN orders o ON oi.order_id = o.id JOIN products p ON oi.product_id = p.id
