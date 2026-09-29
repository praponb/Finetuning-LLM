SELECT id, started_at, LEAD(started_at, 1) OVER (ORDER BY started_at, id) AS next_started_at
FROM sessions
WHERE started_at >= '2025-01-15';
