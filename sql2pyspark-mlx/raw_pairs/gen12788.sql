SELECT
    we.id,
    we.event_type,
    we.page,
    we.event_time
FROM web_events we
WHERE HOUR(we.event_time) BETWEEN 9 AND 17 AND we.session_id NOT IN (SELECT id FROM sessions WHERE browser != 'Chrome')
ORDER BY page, event_time
