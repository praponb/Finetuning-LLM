SELECT
    id,
    browser,
    started_at,
    LEAD(started_at, 1) OVER (PARTITION BY browser ORDER BY started_at, id) next_started_at
FROM sessions
ORDER BY started_at DESC
