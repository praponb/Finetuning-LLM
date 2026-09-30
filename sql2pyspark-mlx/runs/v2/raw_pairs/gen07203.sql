SELECT status, MAX(cost) - MIN(cost) cost_range, ROUND(STDDEV(cost), 2) stddev_cost FROM shipments GROUP BY status;
