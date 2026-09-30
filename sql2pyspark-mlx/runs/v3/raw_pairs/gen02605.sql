SELECT
    s.started_at,
    s.customer_id,
    s.device,
    we.event_time
FROM sessions s
INNER JOIN web_events we ON we.session_id = s.id;
