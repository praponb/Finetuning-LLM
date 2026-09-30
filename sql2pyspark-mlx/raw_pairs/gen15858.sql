SELECT carrier, MAX(cost) - MIN(cost) AS cost_range, COUNT(cost) AS cost_count FROM shipments WHERE status = 'IN_TRANSIT' GROUP BY carrier ORDER BY cost_range DESC
