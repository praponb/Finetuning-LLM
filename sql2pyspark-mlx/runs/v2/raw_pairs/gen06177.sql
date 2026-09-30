SELECT s.id, s.carrier FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee < 17) ORDER BY s.carrier DESC
