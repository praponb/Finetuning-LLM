SELECT
    id,
    session_id,
    event_time,
    page,
    duration_sec,
    RANK() OVER (PARTITION BY session_id ORDER BY event_time) AS event_time_rank,
    LEAD(duration_sec, 1) OVER (PARTITION BY TO_DATE(event_time), page ORDER BY event_time, id) AS next_duration_sec
FROM web_events;
