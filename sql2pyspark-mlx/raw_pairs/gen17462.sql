SELECT id, started_at, DATE_TRUNC('MONTH', started_at) AS started_at_period
FROM sessions
WHERE started_at < '2025-01-01';
