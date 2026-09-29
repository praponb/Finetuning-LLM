SELECT
    p.id,
    p.method,
    p.amount,
    p.paid_at,
    o.discount,
    o.store_id,
    o.channel
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE o.store_id = 28
ORDER BY amount;
