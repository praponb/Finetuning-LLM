SELECT
    sessions.country,
    sessions.device,
    t.home_count,
    t.event_type_values
FROM sessions
LEFT OUTER JOIN (SELECT session_id, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values
    FROM web_events
    WHERE duration_sec > 206
    GROUP BY session_id) t ON sessions.id = t.session_id
ORDER BY sessions.device DESC;
