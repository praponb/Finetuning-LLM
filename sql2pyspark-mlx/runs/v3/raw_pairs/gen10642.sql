SELECT
    id,
    event_time,
    HOUR(event_time) AS event_time_hour,
    CAST(event_time AS DATE) AS event_time_day
FROM web_events
WHERE duration_sec BETWEEN 174 AND 418 AND event_type IN ('click', 'search', 'add_to_cart');
