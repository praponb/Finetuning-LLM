SELECT oi.id, oi.order_id, oi.quantity
FROM order_items oi
WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee BETWEEN 11 AND 25)
