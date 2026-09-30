SELECT s.id, s.started_at, s.country, s.customer_id FROM sessions s WHERE s.device <> 'desktop' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY country;
