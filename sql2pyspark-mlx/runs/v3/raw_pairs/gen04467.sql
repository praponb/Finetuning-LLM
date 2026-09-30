SELECT id, page, event_time, duration_sec, LAG(duration_sec, 1, 0) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_duration_sec FROM web_events;
