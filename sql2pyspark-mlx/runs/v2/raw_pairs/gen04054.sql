SELECT s.id, s.country, s.customer_id FROM sessions s WHERE s.customer_id = 30 AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page NOT IN ('/home', '/product'))
