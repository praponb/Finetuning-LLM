SELECT we.id, we.page
FROM web_events we
WHERE we.duration_sec BETWEEN 188 AND 383 AND we.session_id NOT IN (SELECT id FROM sessions WHERE browser = 'Chrome')
