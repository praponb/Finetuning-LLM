SELECT s.id, s.status FROM shipments s WHERE s.carrier = 'FedEx' AND s.order_id NOT IN (SELECT id FROM orders WHERE amount > 1360)
