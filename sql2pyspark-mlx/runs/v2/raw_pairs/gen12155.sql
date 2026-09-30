SELECT we.id, we.event_time, we.session_id FROM web_events we WHERE we.page != '/home' AND we.session_id IN (SELECT id FROM sessions WHERE TO_DATE(started_at) >= '2023-06-01')
