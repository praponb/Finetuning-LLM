SELECT id, page, event_type FROM web_events WHERE session_id IN (6, 15, 16, 24) INTERSECT SELECT id, page, event_type FROM web_events WHERE event_time >= '2024-08-05'
