SELECT
    duration_sec,
    event_type,
    event_time,
    ROUND(duration_sec * 1.1, 2) AS duration_sec_adjusted
FROM web_events
WHERE duration_sec < 188 OR TO_DATE(event_time) < '2024-05-01';
