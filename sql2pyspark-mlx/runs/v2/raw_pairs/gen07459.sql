SELECT o.id, o.customer_id, o.status
FROM orders o
WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.method = 'paypal')
