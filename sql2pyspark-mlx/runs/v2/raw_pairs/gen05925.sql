SELECT id, browser, started_at, DENSE_RANK() OVER (PARTITION BY browser ORDER BY started_at) started_at_dense_rank FROM sessions;
