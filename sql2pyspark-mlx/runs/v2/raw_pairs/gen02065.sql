SELECT
    id,
    session_id,
    page,
    event_time,
    DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
FROM web_events
WHERE HOUR(event_time) BETWEEN 7 AND 14
