SELECT s.browser, COUNT(we.id) AS web_event_count, MAX(we.event_time) AS last_event_time
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE we.event_type NOT IN ('add_to_cart', 'page_view')
GROUP BY s.browser
ORDER BY web_event_count DESC, last_event_time, browser
LIMIT 3;
