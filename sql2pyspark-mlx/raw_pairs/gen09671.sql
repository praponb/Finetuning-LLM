SELECT
    we.id,
    we.event_time,
    s.customer_id,
    s.device
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
ORDER BY customer_id DESC
