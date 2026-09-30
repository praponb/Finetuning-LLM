SELECT
    we.id,
    we.event_type,
    we.session_id,
    we.duration_sec
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE TO_DATE(started_at) >= '2025-01-01');
