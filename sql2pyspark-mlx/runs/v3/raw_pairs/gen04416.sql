SELECT duration_sec, page, event_type
FROM web_events
WHERE (page = '/home' AND session_id IN (25, 26, 32)) OR event_type = 'add_to_cart'
ORDER BY event_type
