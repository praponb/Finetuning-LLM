SELECT
    s.id,
    s.started_at,
    t.avg_duration_sec,
    t.unique_sessions
FROM sessions s
LEFT OUTER JOIN (SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, COUNT(DISTINCT duration_sec) AS unique_sessions
    FROM web_events
    WHERE event_type = 'purchase'
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY id DESC
