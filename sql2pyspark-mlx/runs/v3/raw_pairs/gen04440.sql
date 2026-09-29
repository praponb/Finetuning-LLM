SELECT id, event_type, event_time, COALESCE(page, 'unassigned') AS page_filled FROM web_events WHERE session_id IN (3, 5, 25) AND session_id IN (22, 26, 27)
