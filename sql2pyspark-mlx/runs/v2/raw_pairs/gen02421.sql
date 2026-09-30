SELECT id, page, session_id, LOWER(event_type) AS event_type_lower FROM web_events WHERE session_id IN (3, 38) ORDER BY session_id, id LIMIT 5
