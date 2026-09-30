SELECT o.status, MAX(s.cost) AS max_cost, COUNT(DISTINCT s.cost) AS unique_orders
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
WHERE s.cost < 33
GROUP BY o.status
ORDER BY max_cost, status
LIMIT 5;
