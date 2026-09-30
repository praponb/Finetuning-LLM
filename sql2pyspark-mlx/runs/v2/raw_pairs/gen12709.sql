SELECT s.id, s.country, s.device, s.customer_id FROM sessions s WHERE s.device = 'desktop' AND s.id IN (SELECT session_id FROM web_events)
