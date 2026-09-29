SELECT o.id, o.amount, o.customer_id
FROM orders o
WHERE o.shipping_fee > 38 AND EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
