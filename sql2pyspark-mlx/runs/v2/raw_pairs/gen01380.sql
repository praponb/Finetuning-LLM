SELECT * FROM (SELECT id, browser, started_at, country, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS row_num FROM sessions) AS r WHERE row_num <= 5
