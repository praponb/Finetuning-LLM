SELECT id, session_id FROM web_events WHERE page != '/home' INTERSECT SELECT id, session_id FROM web_events WHERE event_time >= '2025-10-18'
