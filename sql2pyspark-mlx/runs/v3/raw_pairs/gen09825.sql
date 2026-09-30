SELECT
    p.id,
    p.paid_at,
    p.order_id,
    o.order_date,
    o.customer_id,
    o.channel
FROM payments p
JOIN orders o ON o.id = p.order_id
WHERE o.discount < 0.11
