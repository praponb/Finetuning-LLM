SELECT
    s.carrier,
    o.channel,
    o.customer_id,
    o.status
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE o.discount IS NULL;
