SELECT o.channel, oi.product_id, oi.unit_price FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id ORDER BY oi.product_id
