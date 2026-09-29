SELECT
    s.started_at,
    we.page,
    we.event_time,
    we.event_type
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE s.browser NOT IN ('Edge', 'Firefox')
ORDER BY we.event_type DESC
