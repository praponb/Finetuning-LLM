SELECT page, session_id FROM web_events WHERE event_time < '2024-01-01' AND HOUR(event_time) BETWEEN 10 AND 13;
