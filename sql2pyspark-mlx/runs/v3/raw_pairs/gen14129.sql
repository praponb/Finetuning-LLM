SELECT s.started_at, t.avg_duration_sec, t.num_web_events
FROM sessions s
INNER JOIN (SELECT session_id, AVG(duration_sec) avg_duration_sec, COUNT(*) num_web_events
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.avg_duration_sec >= 420
