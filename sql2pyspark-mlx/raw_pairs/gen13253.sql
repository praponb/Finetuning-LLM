SELECT
    s.id,
    s.delivered_date,
    s.shipped_date,
    o.store_id,
    o.status
FROM shipments s
FULL OUTER JOIN orders o ON o.id = s.order_id
ORDER BY s.delivered_date, store_id;
