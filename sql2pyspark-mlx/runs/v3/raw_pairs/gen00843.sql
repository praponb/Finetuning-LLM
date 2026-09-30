SELECT
    id,
    browser,
    started_at,
    LAG(started_at, 1) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
FROM sessions
WHERE country = 'UK';
