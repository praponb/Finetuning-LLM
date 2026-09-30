SELECT s.id, s.browser, s.device FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE duration_sec > 436);
