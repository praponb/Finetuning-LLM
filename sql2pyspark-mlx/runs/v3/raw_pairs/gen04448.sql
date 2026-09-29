SELECT id, carrier, shipped_date, cost, LEAD(cost, 1) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS next_cost FROM shipments WHERE carrier IS NOT NULL ORDER BY carrier;
