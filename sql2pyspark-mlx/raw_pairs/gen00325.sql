SELECT p.id, p.status
FROM payments p
WHERE p.status NOT IN ('FAILED', 'REFUNDED') AND p.order_id IN (SELECT id FROM orders WHERE amount > 680)
ORDER BY status, id DESC;
