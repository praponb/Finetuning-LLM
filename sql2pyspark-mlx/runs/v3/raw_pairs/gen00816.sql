SELECT id, status, shipped_date, cost, LEAD(cost, 1) OVER (PARTITION BY status ORDER BY shipped_date, id) AS next_cost FROM shipments
