SELECT MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
WHERE session_id IN (14, 15, 27, 34) AND (duration_sec <= 282 OR TO_DATE(event_time) < '2024-10-12')
ORDER BY duration_sec_range DESC
