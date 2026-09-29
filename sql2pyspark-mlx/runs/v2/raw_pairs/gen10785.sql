SELECT id, event_type, session_id, duration_sec, LOWER(event_type) AS event_type_lower FROM web_events WHERE duration_sec BETWEEN 243 AND 266 OR duration_sec <> 265
