SELECT o.id, o.shipping_fee
FROM orders o
WHERE o.id IN (SELECT order_id FROM payments)
