SELECT
    s.id,
    s.device,
    s.country,
    t.min_event_time,
    t.search_duration_sec
FROM sessions s
JOIN (SELECT session_id, MIN(event_time) AS min_event_time, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.search_duration_sec >= 75
ORDER BY t.min_event_time DESC, id
LIMIT 3
