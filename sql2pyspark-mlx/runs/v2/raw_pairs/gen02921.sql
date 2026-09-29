SELECT
    sessions.id,
    sessions.started_at,
    sessions.browser,
    sessions.customer_id
FROM sessions
WHERE NOT EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id)
ORDER BY sessions.id DESC
