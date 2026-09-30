SELECT duration_sec, event_type
FROM web_events
WHERE duration_sec < 154
UNION
SELECT duration_sec, event_type
FROM web_events
WHERE TO_DATE(event_time) < '2024-03-19';
