SELECT
    we.id,
    we.duration_sec,
    we.event_time,
    s.device
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE NOT (s.customer_id = 15 AND s.browser IN ('Chrome', 'Firefox'))
ORDER BY id DESC
