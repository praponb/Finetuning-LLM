SELECT web_events.id, web_events.event_type FROM web_events WHERE web_events.session_id IN (SELECT id FROM sessions WHERE customer_id IS NOT NULL) ORDER BY web_events.id DESC;
