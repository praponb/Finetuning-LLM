SELECT
    s.device,
    MONTH(s.started_at) AS started_month,
    MIN(we.event_time) AS first_event_time,
    AVG(we.duration_sec) AS avg_duration_sec
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY s.device, MONTH(s.started_at)
ORDER BY avg_duration_sec;
