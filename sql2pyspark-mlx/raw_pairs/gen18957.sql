SELECT p.id, p.paid_at, p.order_id FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE channel <> 'mobile')
