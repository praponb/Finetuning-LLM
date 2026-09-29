SELECT id, status, cost, delivered_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS pos FROM shipments) AS t WHERE pos <= 3;
