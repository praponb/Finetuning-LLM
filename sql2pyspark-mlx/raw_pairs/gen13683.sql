SELECT id, cost, ROW_NUMBER() OVER (ORDER BY cost, id) AS rn FROM shipments
