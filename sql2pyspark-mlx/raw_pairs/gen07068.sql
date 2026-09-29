SELECT status, ROUND(STDDEV(cost), 2) AS stddev_cost, COUNT(DISTINCT cost) AS unique_statuss, SUM(cost) AS total_cost FROM shipments GROUP BY status ORDER BY status;
