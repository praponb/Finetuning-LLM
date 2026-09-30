SELECT o.id, o.amount FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND HOUR(p.paid_at) BETWEEN 11 AND 14)
