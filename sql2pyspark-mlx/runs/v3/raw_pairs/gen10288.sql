SELECT we.id, we.page, we.duration_sec FROM web_events we WHERE we.event_type = 'add_to_cart' AND we.session_id IN (SELECT id FROM sessions WHERE TO_DATE(started_at) >= '2024-08-15')
