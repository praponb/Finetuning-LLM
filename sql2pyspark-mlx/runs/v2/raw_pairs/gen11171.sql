SELECT o.id, o.discount, o.customer_id FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.paid_at < '2025-07-01')
