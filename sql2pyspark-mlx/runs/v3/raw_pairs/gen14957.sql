SELECT
    p.id,
    p.amount,
    p.method,
    p.status
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE customer_id IN (16, 26))
