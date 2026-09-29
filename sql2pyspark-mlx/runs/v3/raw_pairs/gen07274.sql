SELECT *, COALESCE(cost, 0) AS cost_filled
FROM shipments
ORDER BY shipped_date, cost_filled DESC
