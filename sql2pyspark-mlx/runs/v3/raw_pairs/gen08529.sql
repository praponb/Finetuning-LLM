SELECT s.id, s.delivered_date, s.carrier FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE discount >= 0.13) ORDER BY s.id;
