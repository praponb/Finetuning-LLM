SELECT status, ROUND(AVG(cost), 2) AS avg_cost, COUNT(DISTINCT cost) AS unique_orders, MAX(cost) - MIN(cost) AS cost_range FROM shipments GROUP BY 1
