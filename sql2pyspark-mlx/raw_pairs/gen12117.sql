SELECT s.id, s.carrier, s.status, s.order_id FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE MONTH(order_date) IN (4, 5, 11)) ORDER BY order_id;
