SELECT p.id, p.status, p.method, p.amount FROM payments p WHERE p.status = 'FAILED' AND p.order_id IN (SELECT id FROM orders WHERE shipping_fee >= 28);
