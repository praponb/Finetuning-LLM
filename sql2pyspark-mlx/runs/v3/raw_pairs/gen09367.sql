SELECT s.id, s.device, s.customer_id, s.country FROM sessions s WHERE s.country = 'US' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (10, 14, 15))
