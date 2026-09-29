SELECT we.id, we.event_time, s.device
FROM web_events we
LEFT OUTER JOIN sessions s ON we.session_id = s.id
WHERE we.page = '/product' OR s.country IN ('SG', 'UK')
ORDER BY id
LIMIT 3
