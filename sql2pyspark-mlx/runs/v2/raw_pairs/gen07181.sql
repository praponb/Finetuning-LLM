SELECT event_type, UPPER(event_type) AS event_type_upper FROM web_events WHERE session_id IN (3, 18, 31, 40) ORDER BY event_type_upper, event_type;
