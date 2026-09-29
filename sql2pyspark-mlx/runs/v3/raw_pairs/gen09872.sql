SELECT s.id, s.country, s.customer_id FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE HOUR(event_time) BETWEEN 11 AND 15) ORDER BY s.id DESC;
