SELECT carrier, MAX(cost) - MIN(cost) AS cost_range FROM shipments GROUP BY carrier;
