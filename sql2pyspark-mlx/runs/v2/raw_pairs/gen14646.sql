SELECT o.order_date, o.channel, oi.order_id, oi.unit_price, oi.product_id FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id
