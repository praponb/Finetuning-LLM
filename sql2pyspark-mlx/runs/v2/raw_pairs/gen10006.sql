SELECT id, status, cost FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS rn FROM shipments) AS t WHERE rn <= 2
