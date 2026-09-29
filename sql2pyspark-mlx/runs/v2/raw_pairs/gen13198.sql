SELECT
    we.id,
    we.event_type,
    we.event_time,
    we.page,
    s.customer_id
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE s.browser = 'Edge'
ORDER BY we.event_time DESC;
