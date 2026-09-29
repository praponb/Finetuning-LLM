WITH session_stats AS (
    SELECT session_id, MAX(duration_sec) max_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) page_values
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    ss.max_duration_sec,
    ss.page_values
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
ORDER BY s.id, ss.page_values
LIMIT 1;
