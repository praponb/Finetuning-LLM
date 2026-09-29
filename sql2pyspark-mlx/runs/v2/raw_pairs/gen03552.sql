SELECT carrier, status, COUNT(cost) AS cost_count FROM shipments WHERE status = 'LOST' AND shipped_date > '2023-11-01' GROUP BY carrier, status HAVING cost_count >= 2
