SELECT s.id, s.browser, t.total_duration_sec
FROM sessions s
JOIN (SELECT session_id, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    WHERE TO_DATE(event_time) < '2024-07-01'
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.total_duration_sec > 1100
ORDER BY s.browser DESC
