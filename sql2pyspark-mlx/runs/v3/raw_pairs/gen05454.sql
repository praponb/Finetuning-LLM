SELECT id, page, duration_sec, MIN(duration_sec) OVER (PARTITION BY page) AS page_min_duration_sec FROM web_events WHERE session_id = 20;
