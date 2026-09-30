SELECT
    o.id,
    o.channel,
    o.order_date,
    o.store_id
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id);
