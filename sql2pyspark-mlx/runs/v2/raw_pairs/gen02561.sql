SELECT id, page, duration_sec FROM web_events WHERE page IS NOT NULL INTERSECT SELECT id, page, duration_sec FROM web_events WHERE session_id IN (26, 40);
