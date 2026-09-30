SELECT
    sessions.id,
    sessions.country,
    t.avg_duration_sec,
    t.duration_sec_count
FROM sessions
LEFT JOIN (SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    WHERE event_time < '2023-06-01'
    GROUP BY session_id) t ON sessions.id = t.session_id
WHERE t.avg_duration_sec > 26
ORDER BY t.avg_duration_sec DESC
