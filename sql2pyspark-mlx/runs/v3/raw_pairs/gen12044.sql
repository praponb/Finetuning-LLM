SELECT o.id, o.customer_id FROM orders o WHERE o.channel = 'mobile' AND o.id IN (SELECT order_id FROM payments)
