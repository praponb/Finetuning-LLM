SELECT
    s.customer_id,
    we.event_time,
    we.page,
    we.duration_sec
FROM sessions s
JOIN web_events we ON we.session_id = s.id
WHERE s.browser = 'Safari'
ORDER BY we.duration_sec DESC
