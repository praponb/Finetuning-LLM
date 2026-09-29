SELECT p.id, p.status FROM payments p WHERE p.paid_at >= '2024-10-05' AND p.order_id IN (SELECT id FROM orders WHERE discount > 0.34)
