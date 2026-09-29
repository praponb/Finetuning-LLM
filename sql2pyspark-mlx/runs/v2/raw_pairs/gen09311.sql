SELECT
    id,
    event_time,
    duration_sec,
    SUM(duration_sec) OVER (ORDER BY event_time, id) AS running_duration_sec
FROM web_events
WHERE page IN ('/home', '/cart', '/checkout');
