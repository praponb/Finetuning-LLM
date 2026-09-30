SELECT session_id, page
FROM web_events
WHERE event_type NOT IN ('purchase', 'page_view')
UNION
SELECT session_id, page
FROM web_events
WHERE duration_sec < 138
ORDER BY session_id
