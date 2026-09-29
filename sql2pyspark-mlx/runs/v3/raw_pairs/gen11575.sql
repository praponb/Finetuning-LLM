SELECT s.id, s.status, s.carrier FROM shipments s WHERE s.cost <= 79 AND s.order_id IN (SELECT id FROM orders WHERE shipping_fee <= 22);
