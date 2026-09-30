WITH session_stats AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.browser,
    s.country,
    ss.event_type_values,
    ss.duration_sec_count
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.duration_sec_count > 4
