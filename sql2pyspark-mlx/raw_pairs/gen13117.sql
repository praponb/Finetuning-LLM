SELECT
    sessions.id,
    sessions.browser,
    sessions.country,
    sessions.device
FROM sessions
WHERE sessions.customer_id = 27 AND EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id);
