SELECT
    id,
    event_time,
    duration_sec,
    event_type,
    LEAD(duration_sec, 1) OVER (ORDER BY event_time, id) AS next_duration_sec,
    DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_time) AS event_time_dense_rank
FROM web_events
WHERE page = '/search';
