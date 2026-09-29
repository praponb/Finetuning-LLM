SELECT duration_sec, event_type, page, ROUND(duration_sec * 0.9, 2) AS duration_sec_adjusted FROM web_events;
