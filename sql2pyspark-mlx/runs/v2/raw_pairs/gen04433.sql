SELECT s.id, s.started_at, s.browser, s.device FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events) ORDER BY browser;
