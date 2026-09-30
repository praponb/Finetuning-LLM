SELECT s.id, s.customer_id, s.country, s.device FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page IN ('/home', '/product', '/cart'))
