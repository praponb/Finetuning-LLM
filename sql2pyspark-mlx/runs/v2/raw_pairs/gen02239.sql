SELECT we.id, we.session_id
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE device IN ('mobile', 'desktop', 'tablet'))
