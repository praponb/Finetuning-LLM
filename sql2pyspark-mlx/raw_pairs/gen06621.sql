SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS rnk FROM sessions) AS t WHERE rnk = 1 ORDER BY started_at
