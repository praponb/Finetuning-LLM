SELECT
    sessions.id,
    sessions.country,
    sessions.customer_id,
    sessions.started_at
FROM sessions
WHERE sessions.id IN (SELECT session_id FROM web_events)
ORDER BY sessions.started_at
