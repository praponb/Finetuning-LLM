SELECT event_type, FLOOR(duration_sec) AS duration_sec_whole, DATE_TRUNC('MONTH', event_time) AS event_time_period
FROM web_events
WHERE NOT (session_id IN (4, 9, 23, 31) AND (page = '/product' OR event_time < '2024-11-01'))
ORDER BY event_type;
