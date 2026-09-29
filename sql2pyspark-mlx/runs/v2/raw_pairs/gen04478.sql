SELECT
    p.amount,
    p.paid_at,
    p.order_id,
    o.status,
    o.discount
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE p.order_id IS NULL AND p.amount <= 1570
