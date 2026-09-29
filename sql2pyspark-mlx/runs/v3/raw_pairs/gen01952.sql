SELECT
    s.id,
    s.delivered_date,
    s.shipped_date,
    s.carrier,
    o.status,
    o.order_date,
    o.discount
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE s.delivered_date BETWEEN '2023-10-10' AND '2025-01-01'
