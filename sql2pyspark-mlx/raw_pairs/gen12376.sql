SELECT
    s.carrier,
    s.cost,
    s.shipped_date,
    o.channel,
    o.store_id
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE o.discount IS NOT NULL
ORDER BY s.shipped_date DESC
