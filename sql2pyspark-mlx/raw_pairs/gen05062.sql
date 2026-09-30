SELECT
    we.id,
    we.event_type,
    we.duration_sec,
    we.session_id,
    s.device,
    s.customer_id,
    s.started_at
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.event_type IN ('search', 'page_view', 'add_to_cart')
ORDER BY device;
