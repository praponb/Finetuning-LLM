SELECT *, CAST(cost AS INT) AS cost_int FROM shipments WHERE status IN ('RETURNED', 'IN_TRANSIT')
