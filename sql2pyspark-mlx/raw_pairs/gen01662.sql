SELECT
    s.id,
    s.status,
    s.cost,
    o.store_id,
    o.shipping_fee
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
