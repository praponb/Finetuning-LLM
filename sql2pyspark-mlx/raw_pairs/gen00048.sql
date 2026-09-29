SELECT
    id,
    event_time,
    session_id,
    duration_sec,
    page,
    event_type,
    RANK() OVER (PARTITION BY MONTH(event_time), session_id ORDER BY duration_sec DESC) AS rnk,
    LAST_VALUE(event_type) OVER (PARTITION BY page, session_id ORDER BY event_time DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_type
FROM web_events
ORDER BY last_event_type, page;
