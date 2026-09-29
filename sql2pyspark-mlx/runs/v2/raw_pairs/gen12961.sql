SELECT id, status, cost, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS row_num FROM shipments;
