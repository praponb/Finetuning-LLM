SELECT o.store_id, o.discount, oi.product_id, oi.quantity FROM orders o LEFT OUTER JOIN order_items oi ON o.id = oi.order_id WHERE o.discount IS NOT NULL AND oi.product_id IS NOT NULL
