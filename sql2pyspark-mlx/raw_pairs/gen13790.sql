SELECT
    we.id,
    we.session_id,
    we.event_type,
    we.event_time
FROM web_events we
WHERE we.event_type IN ('click', 'search') AND we.session_id NOT IN (SELECT id FROM sessions WHERE country = 'JP')
