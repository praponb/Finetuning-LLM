SELECT o.store_id, MAX(s.cost) - MIN(s.cost) AS cost_range
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY o.store_id
