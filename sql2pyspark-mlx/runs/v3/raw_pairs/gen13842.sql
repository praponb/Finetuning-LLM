SELECT id, status, cost, MAX(cost) OVER (PARTITION BY status) AS status_max_cost FROM shipments;
