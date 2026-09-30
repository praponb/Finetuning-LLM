SELECT o.discount, o.channel, oi.quantity
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
