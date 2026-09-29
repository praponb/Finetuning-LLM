SELECT
    id,
    event_time,
    page,
    duration_sec,
    LEAD(duration_sec, 1, 0) OVER (PARTITION BY MONTH(event_time), page ORDER BY event_time, id) AS next_duration_sec,
    MIN(duration_sec) OVER (PARTITION BY page) AS page_min_duration_sec
FROM web_events
WHERE session_id IN (8, 13, 23, 33);
