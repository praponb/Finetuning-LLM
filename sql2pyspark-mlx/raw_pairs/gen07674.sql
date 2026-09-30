SELECT id, status, delivered_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date, id) AS row_num FROM shipments;
