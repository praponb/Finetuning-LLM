WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS row_num FROM sessions) SELECT * FROM ranked WHERE row_num <= 5 ORDER BY row_num ASC
