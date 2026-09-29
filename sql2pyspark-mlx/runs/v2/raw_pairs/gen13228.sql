SELECT page, MIN(event_time) AS min_event_time, MAX(duration_sec) AS max_duration_sec FROM web_events WHERE session_id = 11 GROUP BY 1 ORDER BY page
