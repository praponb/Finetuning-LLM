SELECT id, page, duration_sec, MIN(duration_sec) OVER (PARTITION BY page) AS page_min_duration_sec FROM web_events WHERE event_type = 'purchase' ORDER BY page_min_duration_sec, page;
