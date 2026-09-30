SELECT we.id, we.session_id, we.event_type FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE HOUR(started_at) BETWEEN 6 AND 9)
