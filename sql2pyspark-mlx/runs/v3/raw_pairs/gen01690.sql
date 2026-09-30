SELECT oi.id, oi.order_id
FROM order_items oi
WHERE oi.unit_price >= 390 AND oi.order_id NOT IN (SELECT id FROM orders WHERE amount >= 1350)
