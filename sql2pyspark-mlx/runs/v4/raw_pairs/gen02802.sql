SELECT s.id, s.customer_id, s.country FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events);
