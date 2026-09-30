WITH session_stats AS (
    SELECT session_id, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.browser, sessions.device, session_stats.search_duration_sec
FROM sessions
LEFT OUTER JOIN session_stats ON sessions.id = session_stats.session_id;
