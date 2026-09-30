SELECT id, page, event_time, duration_sec, duration_sec - LAG(duration_sec, 1) OVER (PARTITION BY page ORDER BY event_time, id) duration_sec_change FROM web_events ORDER BY duration_sec_change DESC;
