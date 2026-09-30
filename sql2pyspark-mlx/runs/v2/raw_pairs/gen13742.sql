SELECT s.id, s.country, s.device, s.started_at FROM sessions s WHERE s.country NOT IN ('SG', 'TH', 'UK') AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page IS NOT NULL)
