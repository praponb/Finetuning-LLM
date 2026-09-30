SELECT carrier, MAX(cost) - MIN(cost) AS cost_range, MAX(cost) AS max_cost FROM shipments GROUP BY carrier ORDER BY cost_range;
