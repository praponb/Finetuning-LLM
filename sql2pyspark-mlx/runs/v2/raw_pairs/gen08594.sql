SELECT s.id, s.status FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee > 12)
