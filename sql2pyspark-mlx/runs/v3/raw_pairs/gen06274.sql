SELECT DATE_TRUNC('MONTH', event_time) AS event_month, MIN(event_time) AS min_event_time, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
WHERE session_id IN (22, 30, 40)
GROUP BY 1
ORDER BY min_event_time, event_month
LIMIT 5
