SELECT s.id, s.country, s.started_at, s.browser FROM sessions s WHERE s.country != 'US' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id);
