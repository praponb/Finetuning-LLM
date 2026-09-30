SELECT
    sessions.started_at,
    sessions.browser,
    web_events.event_time,
    web_events.session_id
FROM sessions
LEFT JOIN web_events ON web_events.session_id = sessions.id
WHERE web_events.page = '/search'
ORDER BY sessions.started_at DESC;
