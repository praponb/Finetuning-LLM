SELECT we.id, we.event_time, we.page
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE country = 'US');
