SELECT p.id, p.order_id FROM payments p WHERE p.status = 'SUCCESS' AND p.order_id NOT IN (SELECT id FROM orders WHERE order_date < '2025-09-01');
