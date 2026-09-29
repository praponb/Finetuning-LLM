SELECT duration_sec
FROM web_events
WHERE session_id IN (22, 24)
UNION ALL
SELECT duration_sec
FROM web_events
WHERE event_time < '2024-10-01'
ORDER BY duration_sec;
