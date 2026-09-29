SELECT s.id, s.customer_id, s.device, s.browser FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (10, 20, 24)) ORDER BY customer_id
