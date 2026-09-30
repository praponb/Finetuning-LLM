SELECT id, cost, ROUND(cost, 0) AS cost_rounded FROM shipments WHERE status = 'IN_TRANSIT' AND shipped_date > '2024-08-01' ORDER BY 1
