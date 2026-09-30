SELECT
    s.device,
    s.started_at,
    s.browser,
    we.event_type,
    we.event_time
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.started_at < '2025-09-01' AND s.browser != 'Safari'
ORDER BY we.event_type;
