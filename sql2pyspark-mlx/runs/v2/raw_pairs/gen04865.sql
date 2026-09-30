SELECT s.id, s.country FROM sessions s WHERE s.country != 'SG' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY id, s.country;
