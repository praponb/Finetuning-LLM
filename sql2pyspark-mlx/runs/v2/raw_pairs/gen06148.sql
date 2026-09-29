SELECT
    id,
    event_type,
    event_time,
    session_id,
    LAST_VALUE(session_id) OVER (PARTITION BY event_type ORDER BY event_time DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_session_id
FROM web_events;
