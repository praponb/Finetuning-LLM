SELECT
    p.id,
    p.paid_at,
    o.channel,
    o.shipping_fee,
    UPPER(p.method) AS method_upper
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE p.order_id IN (3, 6, 25) AND p.order_id IS NOT NULL
