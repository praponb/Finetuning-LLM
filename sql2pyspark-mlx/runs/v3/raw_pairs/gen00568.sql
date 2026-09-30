SELECT o.store_id, oi.quantity, COALESCE(o.discount, 0) AS discount_filled
FROM orders o
JOIN order_items oi ON oi.order_id = o.id
