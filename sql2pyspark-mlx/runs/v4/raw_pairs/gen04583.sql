SELECT o.status, ROUND(AVG(s.cost), 2) AS avg_cost, COUNT(CASE WHEN s.carrier = 'UPS' THEN 1 END) AS ups_count
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE o.status = 'COMPLETED'
GROUP BY o.status
ORDER BY avg_cost DESC, status
LIMIT 20;
