SELECT id, country, started_at FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS row_num FROM sessions) AS ranked WHERE row_num <= 2;
