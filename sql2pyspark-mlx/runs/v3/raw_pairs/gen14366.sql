SELECT * FROM web_events WHERE HOUR(event_time) BETWEEN 12 AND 15 AND session_id IN (2, 6);
