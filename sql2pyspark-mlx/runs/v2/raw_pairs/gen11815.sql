SELECT page, AVG(duration_sec) AS avg_duration_sec, MAX(event_time) AS last_event_time FROM web_events GROUP BY page ORDER BY last_event_time
