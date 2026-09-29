SELECT *, LEAD(duration_sec, 1) OVER (PARTITION BY page ORDER BY event_time, id) AS next_duration_sec, NTILE(5) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket FROM web_events;
