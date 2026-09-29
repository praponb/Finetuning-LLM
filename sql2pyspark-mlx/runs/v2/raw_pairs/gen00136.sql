WITH session_stats AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'page_view' THEN duration_sec ELSE 0 END) AS page_view_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.country, session_stats.page_view_duration_sec
FROM sessions
LEFT JOIN session_stats ON sessions.id = session_stats.session_id
ORDER BY sessions.country;
