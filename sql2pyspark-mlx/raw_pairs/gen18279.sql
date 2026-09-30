SELECT
    p.paid_at,
    o.store_id,
    o.discount,
    o.shipping_fee
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
