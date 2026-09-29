SELECT p.id, p.order_id, p.paid_at
FROM payments p
WHERE p.order_id IN (8, 10, 11) AND p.order_id NOT IN (SELECT id FROM orders WHERE amount > 1100)
