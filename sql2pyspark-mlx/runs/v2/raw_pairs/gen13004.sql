SELECT we.id, we.session_id
FROM web_events we
WHERE we.duration_sec >= 262 AND we.session_id IN (SELECT id FROM sessions WHERE country IN ('UK', 'DE', 'US'))
ORDER BY we.session_id DESC, id
LIMIT 10;
