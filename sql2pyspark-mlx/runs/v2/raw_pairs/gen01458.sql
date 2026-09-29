SELECT id, country, started_at, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS started_at_row FROM sessions
