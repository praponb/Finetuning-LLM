SELECT s.id, s.customer_id, s.country FROM sessions s WHERE s.device = 'mobile' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id);
