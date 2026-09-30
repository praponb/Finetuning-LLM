SELECT
    s.id,
    s.browser,
    COUNT(we.id) AS web_event_count,
    MAX(we.event_time) AS last_event_time,
    SORT_ARRAY(COLLECT_SET(we.event_type)) AS event_type_list
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
GROUP BY s.id, s.browser
ORDER BY id DESC, browser
LIMIT 10
