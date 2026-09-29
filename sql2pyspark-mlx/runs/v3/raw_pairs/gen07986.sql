SELECT
    id,
    session_id,
    event_time,
    duration_sec,
    LEAD(duration_sec, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) next_duration_sec
FROM web_events
WHERE TO_DATE(event_time) >= '2023-09-09'
