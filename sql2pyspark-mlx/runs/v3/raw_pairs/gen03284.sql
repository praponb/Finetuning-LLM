SELECT o.status, oi.quantity FROM orders o INNER JOIN order_items oi ON oi.order_id = o.id
