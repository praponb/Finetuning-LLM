SELECT
    o.store_id,
    o.status,
    MAX(s.cost) - MIN(s.cost) AS cost_range,
    MAX(s.delivered_date) AS last_delivered_date
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE o.order_date >= '2025-10-30'
GROUP BY o.store_id, o.status
ORDER BY last_delivered_date DESC;
