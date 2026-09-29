SELECT we.id, we.page, we.event_time
FROM web_events we
WHERE we.session_id IN (9, 21, 27) AND we.session_id NOT IN (SELECT id FROM sessions WHERE country = 'SG')
