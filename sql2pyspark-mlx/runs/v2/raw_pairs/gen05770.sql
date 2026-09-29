SELECT p.id, p.method
FROM payments p
WHERE p.amount BETWEEN 500 AND 1400 AND p.order_id IN (SELECT id FROM orders WHERE channel IN ('store', 'web'));
