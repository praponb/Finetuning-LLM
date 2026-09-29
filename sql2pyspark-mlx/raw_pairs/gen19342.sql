SELECT we.id, we.event_time
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE country IN ('DE', 'TH'))
