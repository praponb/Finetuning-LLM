SELECT
    id,
    page,
    event_time,
    duration_sec,
    LEAD(duration_sec, 1) OVER (PARTITION BY page ORDER BY event_time, id) next_duration_sec,
    COUNT(*) OVER (PARTITION BY page) page_count
FROM web_events
WHERE session_id IN (3, 24, 30, 34)
