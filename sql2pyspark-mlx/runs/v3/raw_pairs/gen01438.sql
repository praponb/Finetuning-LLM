SELECT s.id, s.cost, s.status FROM shipments s WHERE s.cost >= 77 AND s.order_id IN (SELECT id FROM orders WHERE shipping_fee >= 11)
