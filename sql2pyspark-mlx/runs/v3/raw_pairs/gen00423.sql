SELECT s.customer_id, we.session_id, we.event_type, we.duration_sec FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id WHERE s.country = 'UK';
