SELECT s.id, s.customer_id, s.browser FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (10, 11)) ORDER BY s.browser DESC, id LIMIT 50
