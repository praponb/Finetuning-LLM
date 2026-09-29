SELECT p.id, p.method, p.status FROM payments p WHERE TO_DATE(p.paid_at) >= '2024-11-01' AND p.order_id IN (SELECT id FROM orders WHERE channel IN ('partner', 'web'));
