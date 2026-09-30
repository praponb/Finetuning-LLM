SELECT s.id, s.browser FROM sessions s WHERE s.customer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
