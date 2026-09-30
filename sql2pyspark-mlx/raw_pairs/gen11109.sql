SELECT
    id,
    device,
    browser,
    started_at,
    HOUR(started_at) AS started_at_hour
FROM sessions
