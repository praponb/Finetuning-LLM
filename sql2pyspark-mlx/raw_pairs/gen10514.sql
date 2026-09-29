SELECT p.id, p.method, p.order_id FROM payments p WHERE p.status = 'FAILED' AND p.order_id IN (SELECT id FROM orders WHERE shipping_fee <= 16)
