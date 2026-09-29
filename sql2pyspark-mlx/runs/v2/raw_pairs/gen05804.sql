SELECT we.id, we.event_time
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE device = 'tablet')
ORDER BY we.id
LIMIT 1
