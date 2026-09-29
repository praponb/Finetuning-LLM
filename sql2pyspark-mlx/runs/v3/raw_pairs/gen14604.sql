SELECT id, started_at, RANK() OVER (ORDER BY started_at DESC) AS started_at_rank
FROM sessions
ORDER BY started_at;
