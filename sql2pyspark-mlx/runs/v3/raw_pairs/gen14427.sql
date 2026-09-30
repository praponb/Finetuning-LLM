SELECT s.id, s.started_at FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_type NOT IN ('click', 'search')) ORDER BY 2, 1;
