SELECT o.status, SUM(s.cost) total_cost, SUM(CASE WHEN s.carrier = 'Flash' THEN s.cost ELSE 0 END) flash_cost
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
GROUP BY o.status
ORDER BY flash_cost DESC
