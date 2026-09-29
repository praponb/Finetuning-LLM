SELECT
    id,
    event_time,
    event_type,
    DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
FROM web_events
WHERE page = '/product' AND page = '/home'
ORDER BY event_time_label, event_type DESC;
