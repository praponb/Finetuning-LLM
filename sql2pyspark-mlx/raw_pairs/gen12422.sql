SELECT p.id, p.status, p.paid_at, p.method FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE order_date BETWEEN '2024-06-01' AND '2025-02-19');
