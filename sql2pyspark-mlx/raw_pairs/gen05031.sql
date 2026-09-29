SELECT
    p.id,
    p.status,
    o.amount,
    o.customer_id,
    o.channel
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE p.amount > 1480
