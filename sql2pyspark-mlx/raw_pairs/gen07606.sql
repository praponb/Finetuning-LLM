SELECT page, event_time, ROUND(duration_sec, 0) AS duration_sec_rounded, LENGTH(event_type) AS event_type_length FROM web_events WHERE session_id IN (3, 24, 37) ORDER BY page, event_type_length DESC
