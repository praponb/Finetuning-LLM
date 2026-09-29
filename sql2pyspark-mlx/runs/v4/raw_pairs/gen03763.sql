SELECT we.id, we.event_type, we.event_time, we.page FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE TO_DATE(started_at) < '2025-10-06');
