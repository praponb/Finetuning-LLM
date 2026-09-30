SELECT id, shipped_date, carrier, CEIL(cost) AS cost_whole FROM shipments ORDER BY id DESC;
