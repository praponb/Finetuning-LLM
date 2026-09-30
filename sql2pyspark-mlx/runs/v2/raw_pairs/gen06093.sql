SELECT s.id, s.browser, s.country, s.customer_id FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY customer_id DESC;
