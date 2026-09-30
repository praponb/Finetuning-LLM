SELECT
    s.customer_id,
    s.country,
    we.event_time,
    we.event_type,
    we.page
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE s.browser = 'Safari' OR we.session_id IN (31, 36, 37, 39)
ORDER BY we.event_time;
