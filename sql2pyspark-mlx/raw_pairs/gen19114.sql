SELECT
    id,
    country,
    browser,
    started_at,
    LEAD(started_at, 1) OVER (PARTITION BY country, browser ORDER BY started_at, id) AS next_started_at
FROM sessions;
