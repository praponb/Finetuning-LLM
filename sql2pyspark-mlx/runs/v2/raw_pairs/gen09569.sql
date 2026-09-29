SELECT * FROM (SELECT session_id, page, duration_sec FROM web_events WHERE duration_sec IS NOT NULL) PIVOT (AVG(duration_sec) FOR page IN ('/product', '/search'));
