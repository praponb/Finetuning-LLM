SELECT s.id, s.browser FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE event_time >= '2025-09-01');
