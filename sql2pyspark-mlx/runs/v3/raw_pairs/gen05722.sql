SELECT id, browser, country, started_at, ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY started_at, id) started_at_row FROM sessions;
