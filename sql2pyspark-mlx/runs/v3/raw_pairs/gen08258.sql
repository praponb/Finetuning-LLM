SELECT id, started_at, PERCENT_RANK() OVER (ORDER BY started_at) AS started_at_pct_rank
FROM sessions
ORDER BY started_at;
