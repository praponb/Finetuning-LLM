SELECT s.id, s.customer_id, s.country, s.started_at FROM sessions s WHERE s.country = 'TH' AND s.id IN (SELECT session_id FROM web_events);
