SELECT
    session_id,
    MIN(event_time) first_event_time,
    MIN(event_time) min_event_time,
    AVG(duration_sec) avg_duration_sec
FROM web_events
GROUP BY session_id
ORDER BY session_id, avg_duration_sec DESC;
