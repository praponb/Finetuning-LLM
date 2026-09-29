SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events WHERE session_id IN (12, 23, 28, 36))
PIVOT (SUM(duration_sec) FOR page IN ('/product', '/search'));
