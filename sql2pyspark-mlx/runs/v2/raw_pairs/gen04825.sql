SELECT
    p.id,
    p.status,
    o.store_id,
    o.shipping_fee,
    o.channel
FROM payments p
FULL OUTER JOIN orders o ON o.id = p.order_id
