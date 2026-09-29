SELECT status, ROUND(STDDEV(cost), 2) AS stddev_cost, MAX(cost) - MIN(cost) AS cost_range FROM shipments WHERE status = 'RETURNED' GROUP BY status
