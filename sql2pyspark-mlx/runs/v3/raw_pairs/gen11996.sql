SELECT
    o.id,
    o.status,
    o.channel,
    o.store_id
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id)
