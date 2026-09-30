SELECT
    page,
    event_time,
    duration_sec,
    CAST(event_time AS DATE) AS event_time_day
FROM web_events
WHERE event_time < '2024-02-01' AND session_id IN (9, 18, 27, 34)
ORDER BY event_time_day DESC
