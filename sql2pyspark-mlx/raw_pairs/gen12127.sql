SELECT
    sessions.customer_id,
    sessions.browser,
    web_events.duration_sec,
    web_events.event_time,
    web_events.event_type
FROM sessions
INNER JOIN web_events ON sessions.id = web_events.session_id
WHERE sessions.customer_id IN (3, 20, 22, 28) AND web_events.page IN ('/cart', '/search', '/checkout')
ORDER BY sessions.customer_id
