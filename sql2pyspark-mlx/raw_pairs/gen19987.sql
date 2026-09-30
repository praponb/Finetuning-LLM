SELECT s.id, s.customer_id, s.browser, s.device FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY s.customer_id DESC;
