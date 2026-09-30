SELECT s.id, s.order_id, s.shipped_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE discount >= 0.33);
