SELECT
    id,
    browser,
    started_at,
    device
FROM sessions
WHERE started_at >= '2025-05-01'
