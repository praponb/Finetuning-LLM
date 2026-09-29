SELECT
    sessions.id,
    sessions.started_at,
    sessions.browser,
    sessions.device
FROM sessions
WHERE sessions.browser IN ('Firefox', 'Safari') AND EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id)
ORDER BY device, id
LIMIT 10
