WITH session_stats AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values
    FROM web_events
    WHERE duration_sec = 431
    GROUP BY session_id
)
SELECT
    sessions.id,
    sessions.customer_id,
    sessions.country,
    session_stats.avg_duration_sec,
    session_stats.event_type_values
FROM sessions
LEFT OUTER JOIN session_stats ON sessions.id = session_stats.session_id
WHERE session_stats.avg_duration_sec > 26
ORDER BY sessions.customer_id
