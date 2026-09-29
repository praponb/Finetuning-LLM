SELECT id, started_at, LEAD(started_at, 1) OVER (ORDER BY started_at, id) AS next_started_at, RANK() OVER (ORDER BY started_at) AS started_at_rank FROM sessions WHERE country = 'UK' ORDER BY 3 DESC;
