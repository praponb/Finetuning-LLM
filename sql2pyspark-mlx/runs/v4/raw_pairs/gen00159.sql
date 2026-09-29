SELECT p.id, p.method FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE order_date >= '2023-11-01') ORDER BY method, id LIMIT 1;
