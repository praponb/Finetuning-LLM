SELECT carrier, status, MAX(cost) - MIN(cost) AS cost_range FROM shipments GROUP BY carrier, status ORDER BY 1, status;
