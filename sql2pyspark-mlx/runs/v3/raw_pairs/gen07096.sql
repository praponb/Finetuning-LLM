SELECT o.amount, oi.product_id, oi.quantity FROM orders o LEFT JOIN order_items oi ON oi.order_id = o.id ORDER BY amount DESC
