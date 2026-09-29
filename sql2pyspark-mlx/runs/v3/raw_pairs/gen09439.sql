SELECT o.status, ROUND(AVG(s.cost), 2) AS avg_cost FROM shipments s INNER JOIN orders o ON s.order_id = o.id GROUP BY o.status ORDER BY avg_cost DESC, status LIMIT 1
