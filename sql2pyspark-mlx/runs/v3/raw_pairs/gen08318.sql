SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    page,
    LAG(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_duration_sec,
    LAST_VALUE(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec
FROM web_events
WHERE page != '/search'
ORDER BY duration_sec DESC;
