SELECT
    id,
    duration_sec,
    session_id,
    page
FROM web_events
WHERE (page = '/home' AND duration_sec > 230) OR TO_DATE(event_time) >= '2025-04-01';
