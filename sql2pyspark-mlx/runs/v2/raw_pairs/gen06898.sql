SELECT
    id,
    event_time,
    event_type,
    duration_sec
FROM web_events
WHERE page = '/cart';
