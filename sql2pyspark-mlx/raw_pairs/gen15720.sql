SELECT s.id, s.browser, s.customer_id FROM sessions s WHERE s.country != 'SG' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY s.customer_id DESC;
