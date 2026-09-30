SELECT s.id, s.carrier, s.delivered_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE customer_id IN (6, 29, 30))
