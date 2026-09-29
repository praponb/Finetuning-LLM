SELECT carrier, MAX(cost) - MIN(cost) AS cost_range, ROUND(STDDEV(cost), 2) AS stddev_cost FROM shipments GROUP BY carrier ORDER BY carrier DESC, cost_range DESC LIMIT 5
