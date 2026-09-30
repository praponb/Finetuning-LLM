SELECT id, status, cost, ROUND(cost / SUM(cost) OVER (PARTITION BY status), 3) cost_share FROM shipments
