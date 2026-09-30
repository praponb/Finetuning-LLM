SELECT
    we.id,
    we.page,
    we.duration_sec,
    we.event_type
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE country = 'SG')
