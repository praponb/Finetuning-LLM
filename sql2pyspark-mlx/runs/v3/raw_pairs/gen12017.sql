SELECT id, country, started_at, RANK() OVER (PARTITION BY country ORDER BY started_at) AS started_at_rank FROM sessions ORDER BY started_at_rank ASC
