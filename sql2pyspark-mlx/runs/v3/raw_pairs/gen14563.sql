WITH session_summary AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values, MAX(event_time) AS last_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.browser,
    ss.page_values,
    ss.last_event_time
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
ORDER BY s.device DESC, id
LIMIT 10
