SELECT
    s.carrier,
    s.cost,
    s.status,
    o.shipping_fee
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id;
