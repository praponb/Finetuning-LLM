SELECT status, COUNT(cost) AS cost_count, AVG(cost) AS avg_cost FROM shipments GROUP BY status;
