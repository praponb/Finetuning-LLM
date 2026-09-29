SELECT id, page, event_time, duration_sec, SUM(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id) AS running_duration_sec FROM web_events WHERE event_type = 'page_view'
