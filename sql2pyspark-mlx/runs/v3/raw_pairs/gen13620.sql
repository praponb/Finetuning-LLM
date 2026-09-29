SELECT
    sessions.id,
    sessions.started_at,
    sessions.customer_id,
    sessions.device
FROM sessions
WHERE sessions.country = 'JP' AND NOT EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id)
ORDER BY sessions.id DESC, customer_id DESC;
