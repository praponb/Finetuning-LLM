SELECT p.id, p.amount, p.paid_at FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE shipping_fee > 29) ORDER BY amount, id LIMIT 1
