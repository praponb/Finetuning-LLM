SELECT s.id, s.country, s.customer_id FROM sessions s WHERE s.device = 'tablet' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page = '/search') ORDER BY s.customer_id;
