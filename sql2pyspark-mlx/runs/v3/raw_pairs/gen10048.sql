SELECT
    s.id,
    s.delivered_date,
    s.shipped_date,
    s.carrier,
    o.store_id
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE YEAR(s.delivered_date) >= 2025 AND s.cost < 35
