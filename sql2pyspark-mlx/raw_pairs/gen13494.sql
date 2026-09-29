SELECT s.id, s.carrier, s.order_id, s.status FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE YEAR(order_date) = 2024) ORDER BY status
