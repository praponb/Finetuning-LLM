SELECT *
FROM web_events
WHERE page = '/product' OR session_id IN (1, 6, 28)
ORDER BY event_type, duration_sec DESC
