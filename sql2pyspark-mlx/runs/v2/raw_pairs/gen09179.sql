SELECT p.id, p.order_id FROM payments p WHERE TO_DATE(p.paid_at) < '2023-04-01' AND p.order_id IN (SELECT id FROM orders WHERE shipping_fee < 21);
