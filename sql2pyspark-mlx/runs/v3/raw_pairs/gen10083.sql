SELECT s.id, s.customer_id, s.country, s.browser FROM sessions s WHERE s.device = 'desktop' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY customer_id DESC;
