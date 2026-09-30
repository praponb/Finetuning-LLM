SELECT s.id, s.delivered_date, s.order_id FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE order_date < '2025-10-01') ORDER BY delivered_date, s.id LIMIT 5;
