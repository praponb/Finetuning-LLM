SELECT o.id, o.discount FROM orders o WHERE o.id IN (SELECT order_id FROM payments);
