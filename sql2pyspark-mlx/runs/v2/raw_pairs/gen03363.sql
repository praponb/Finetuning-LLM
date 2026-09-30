SELECT id, event_time, ROUND(duration_sec * 0.85, 2) AS adjusted_duration_sec FROM web_events WHERE page IS NULL;
