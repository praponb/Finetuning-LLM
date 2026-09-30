SELECT page, event_type, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range FROM web_events WHERE session_id = 38 GROUP BY page, event_type
