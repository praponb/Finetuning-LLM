SELECT id, device, started_at, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) row_num FROM sessions WHERE country = 'SG'
