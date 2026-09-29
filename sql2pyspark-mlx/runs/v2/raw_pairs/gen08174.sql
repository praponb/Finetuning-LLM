SELECT id, status, cost FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS pos FROM shipments) AS ranked WHERE pos <= 3 ORDER BY id DESC;
