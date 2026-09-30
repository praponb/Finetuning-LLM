SELECT
    sessions.id,
    sessions.device,
    COUNT(web_events.id) AS web_event_count,
    MAX(web_events.duration_sec) - MIN(web_events.duration_sec) AS duration_sec_range,
    SUM(web_events.duration_sec) AS total_duration_sec
FROM sessions
LEFT JOIN web_events ON sessions.id = web_events.session_id
GROUP BY sessions.id, sessions.device
ORDER BY duration_sec_range DESC, 5 DESC;
