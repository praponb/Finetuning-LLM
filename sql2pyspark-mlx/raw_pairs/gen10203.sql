SELECT
    we.event_time,
    we.session_id,
    we.duration_sec,
    s.device,
    s.customer_id
FROM web_events we
FULL OUTER JOIN sessions s ON s.id = we.session_id
WHERE we.event_type = 'purchase';
