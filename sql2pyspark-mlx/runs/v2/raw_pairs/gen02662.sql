SELECT MIN(duration_sec) AS min_duration_sec, COUNT(DISTINCT duration_sec) AS unique_sessions
FROM web_events
WHERE TO_DATE(event_time) < '2024-02-01'
ORDER BY min_duration_sec DESC
