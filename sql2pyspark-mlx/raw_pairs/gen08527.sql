SELECT we.id, we.event_type FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE TO_DATE(started_at) < '2023-03-01') ORDER BY we.event_type DESC, we.id DESC;
