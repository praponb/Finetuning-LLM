SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_num FROM sessions) AS t WHERE row_num <= 3 ORDER BY 6 DESC
