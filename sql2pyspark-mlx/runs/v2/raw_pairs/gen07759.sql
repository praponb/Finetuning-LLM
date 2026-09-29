SELECT * FROM (SELECT id, country, started_at, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS row_num FROM sessions) AS r WHERE row_num = 1
