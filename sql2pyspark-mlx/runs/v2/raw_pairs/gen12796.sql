SELECT
    id,
    session_id,
    duration_sec,
    event_time,
    LAST_VALUE(event_time) OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_time
FROM web_events;
