SELECT id, shipped_date, cost, ROW_NUMBER() OVER (PARTITION BY MONTH(shipped_date) ORDER BY cost DESC, id) AS rn FROM shipments
