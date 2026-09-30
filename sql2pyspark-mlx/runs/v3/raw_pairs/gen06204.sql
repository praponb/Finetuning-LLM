SELECT carrier, MAX(cost) - MIN(cost) AS cost_range, COUNT(*) AS cnt FROM shipments GROUP BY 1 HAVING COUNT(*) < 2 ORDER BY cnt
