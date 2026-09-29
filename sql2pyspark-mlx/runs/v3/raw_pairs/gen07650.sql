SELECT id, status, delivered_date, cost FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date, id) row_num FROM shipments) r WHERE row_num = 1
