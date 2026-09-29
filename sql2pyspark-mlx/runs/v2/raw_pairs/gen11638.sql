SELECT p.id, p.paid_at
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.21 AND 0.36)
ORDER BY p.id
LIMIT 20;
