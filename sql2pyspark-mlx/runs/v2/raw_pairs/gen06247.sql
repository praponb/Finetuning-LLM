SELECT s.id, s.customer_id, s.started_at FROM sessions s WHERE s.country NOT IN ('SG', 'JP', 'TH') AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id = 26);
