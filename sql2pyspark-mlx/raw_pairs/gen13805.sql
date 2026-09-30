SELECT
    id,
    session_id,
    event_time,
    duration_sec,
    event_type,
    LAST_VALUE(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
    MIN(duration_sec) OVER (PARTITION BY event_type) AS event_type_min_duration_sec
FROM web_events;
