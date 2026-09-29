SELECT id, status, CAST(cost AS INT) AS cost_int
FROM shipments
ORDER BY status ASC
