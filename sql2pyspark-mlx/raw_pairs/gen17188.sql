SELECT id, event_time, duration_sec FROM web_events WHERE session_id IN (12, 29, 32) AND (session_id IN (6, 7, 9) OR event_type IN ('purchase', 'click'));
