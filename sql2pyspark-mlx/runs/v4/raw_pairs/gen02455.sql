SELECT id, carrier, cost, ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 3) AS cost_share FROM shipments WHERE cost > 80 ORDER BY cost DESC, cost_share DESC
