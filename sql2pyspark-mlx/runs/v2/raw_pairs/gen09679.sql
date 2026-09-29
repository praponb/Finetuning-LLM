SELECT id, status, cost, AVG(cost) OVER (PARTITION BY status) status_avg_cost FROM shipments;
