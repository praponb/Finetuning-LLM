WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) rnk FROM sessions) SELECT * FROM ranked WHERE rnk <= 5 ORDER BY country DESC
