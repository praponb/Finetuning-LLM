SELECT
    sessions.id,
    sessions.browser,
    COUNT(web_events.id) web_event_count,
    COUNT(CASE WHEN web_events.event_type = 'add_to_cart' THEN 1 END) add_to_cart_count,
    COUNT(DISTINCT web_events.duration_sec) unique_event_types
FROM sessions
LEFT JOIN web_events ON sessions.id = web_events.session_id
GROUP BY sessions.id, sessions.browser
HAVING web_event_count > 2
ORDER BY browser
