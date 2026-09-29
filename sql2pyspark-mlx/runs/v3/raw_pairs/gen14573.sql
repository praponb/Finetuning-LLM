SELECT
    id,
    session_id,
    event_time,
    duration_sec,
    LAG(duration_sec, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) prev_duration_sec
FROM web_events
WHERE HOUR(event_time) BETWEEN 9 AND 13;
