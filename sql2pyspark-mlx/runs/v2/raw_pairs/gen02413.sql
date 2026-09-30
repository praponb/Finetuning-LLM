SELECT s.id, s.delivered_date, s.carrier FROM shipments s WHERE s.delivered_date >= '2023-09-01' AND s.order_id IN (SELECT id FROM orders WHERE amount >= 770)
