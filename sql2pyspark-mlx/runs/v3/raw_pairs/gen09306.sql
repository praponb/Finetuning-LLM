SELECT status, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY status) AS cumulative_cost FROM shipments GROUP BY status ORDER BY cumulative_cost ASC;
