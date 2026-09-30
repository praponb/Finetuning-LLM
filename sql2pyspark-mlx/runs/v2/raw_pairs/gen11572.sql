SELECT
    we.id,
    we.page,
    we.session_id,
    we.event_time
FROM web_events we
WHERE we.event_type = 'click' AND we.session_id IN (SELECT id FROM sessions WHERE browser = 'Edge')
ORDER BY id
LIMIT 5;
