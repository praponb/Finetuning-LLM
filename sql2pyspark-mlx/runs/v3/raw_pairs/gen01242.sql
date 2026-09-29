SELECT we.id, we.session_id, we.page FROM web_events we WHERE we.page IN ('/cart', '/checkout', '/home') AND we.session_id IN (SELECT id FROM sessions WHERE browser = 'Chrome')
