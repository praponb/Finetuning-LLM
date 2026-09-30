SELECT status, MAX(cost) AS max_cost FROM shipments GROUP BY status HAVING max_cost >= 81 ORDER BY max_cost DESC, status LIMIT 5;
