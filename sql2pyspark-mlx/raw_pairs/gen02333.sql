SELECT
    we.id,
    we.event_type,
    we.session_id,
    we.event_time
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE device NOT IN ('tablet', 'desktop'));
