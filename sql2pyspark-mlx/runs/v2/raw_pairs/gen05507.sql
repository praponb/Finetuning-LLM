SELECT
    s.id,
    s.delivered_date,
    o.customer_id,
    o.discount,
    LOWER(s.status) AS status_lower
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
WHERE o.order_date < '2023-07-01' AND s.order_id IS NULL;
