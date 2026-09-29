SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS rn FROM sessions) AS r WHERE rn <= 3
