SELECT
    id,
    event_time,
    event_type,
    duration_sec,
    DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time,
    TRIM(event_type) AS clean_event_type
FROM web_events
WHERE event_type = 'purchase';
