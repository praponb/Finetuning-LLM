SELECT o.id, o.customer_id
FROM orders o
WHERE o.status != 'COMPLETED' AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.method != 'paypal');
