SELECT
    we.id,
    we.duration_sec,
    we.session_id,
    we.event_time
FROM web_events we
WHERE we.event_type NOT IN ('search', 'purchase') AND we.session_id IN (SELECT id FROM sessions WHERE country = 'SG')
ORDER BY 4 DESC, id
LIMIT 5;
