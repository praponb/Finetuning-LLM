SELECT * FROM (SELECT status, MAX(cost) AS max_cost FROM shipments GROUP BY status) AS sub WHERE max_cost >= 64 ORDER BY status, max_cost DESC;
