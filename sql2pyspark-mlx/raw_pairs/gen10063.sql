SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events WHERE session_id = 21)
PIVOT (MAX(duration_sec) FOR page IN ('/product', '/cart', '/checkout'))
ORDER BY event_type;
