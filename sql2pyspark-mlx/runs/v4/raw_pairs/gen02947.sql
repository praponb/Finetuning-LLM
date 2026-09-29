SELECT
    id,
    country,
    device,
    started_at,
    LEAD(started_at, 1) OVER (PARTITION BY country, device ORDER BY started_at, id) AS next_started_at
FROM sessions
WHERE started_at < '2025-01-01';
