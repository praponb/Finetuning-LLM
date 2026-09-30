SELECT p.id, p.paid_at, p.status
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE customer_id IS NOT NULL)
ORDER BY p.id DESC
