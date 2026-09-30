SELECT status, AVG(cost) AS avg_cost, MIN(cost) AS min_cost FROM shipments WHERE cost <= 91 AND delivered_date > '2023-03-08' GROUP BY status ORDER BY avg_cost;
