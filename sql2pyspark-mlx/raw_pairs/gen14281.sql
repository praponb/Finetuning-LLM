SELECT s.id, s.status, s.carrier FROM shipments s WHERE s.shipped_date BETWEEN '2023-05-01' AND '2023-10-01' AND s.order_id IN (SELECT id FROM orders WHERE customer_id = 19) ORDER BY s.status
