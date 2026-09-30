SELECT
    id,
    browser,
    started_at,
    country,
    LAG(started_at, 1) OVER (PARTITION BY browser ORDER BY started_at, id) prev_started_at,
    LAG(started_at, 1) OVER (PARTITION BY country ORDER BY started_at, id) prev_started_at_2
FROM sessions;
