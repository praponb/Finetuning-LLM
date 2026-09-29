WITH top_sessions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS pos FROM sessions) SELECT * FROM top_sessions WHERE pos <= 3;
