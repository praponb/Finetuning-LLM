SELECT
    s.id,
    s.delivered_date,
    o.store_id,
    o.order_date
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
WHERE s.status IN ('DELIVERED', 'LOST');
