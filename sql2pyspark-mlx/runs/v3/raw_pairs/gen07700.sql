SELECT
    s.id,
    s.status,
    s.order_id,
    s.carrier,
    o.amount,
    o.order_date
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id;
