SELECT page, MAX(event_time) AS latest_event_time, MIN(event_time) AS first_event_time, AVG(duration_sec) AS avg_duration_sec FROM web_events GROUP BY page ORDER BY page, latest_event_time
