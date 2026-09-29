SELECT
    s.device,
    s.browser,
    s.customer_id,
    we.event_type,
    we.session_id,
    we.event_time
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE we.session_id = 22 AND we.event_type = 'purchase'
