SELECT p.id, p.method, p.paid_at
FROM payments p
WHERE p.amount < 830 AND p.order_id IN (SELECT id FROM orders WHERE amount > 450)
