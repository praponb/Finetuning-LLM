SELECT
    s.id,
    s.status,
    o.discount,
    o.customer_id
FROM shipments s
LEFT OUTER JOIN orders o ON o.id = s.order_id;
