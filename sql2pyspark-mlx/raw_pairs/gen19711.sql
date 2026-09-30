SELECT s.id, s.customer_id FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events)
