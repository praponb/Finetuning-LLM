SELECT o.channel, oi.order_id, oi.unit_price, oi.quantity FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id WHERE o.channel IN ('web', 'store') ORDER BY oi.quantity DESC;
