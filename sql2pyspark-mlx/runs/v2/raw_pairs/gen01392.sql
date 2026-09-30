WITH session_stats AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    WHERE event_type NOT IN ('page_view', 'search')
    GROUP BY session_id
)
SELECT s.started_at, s.country, ss.avg_duration_sec
FROM sessions s
LEFT JOIN session_stats ss ON s.id = ss.session_id;
