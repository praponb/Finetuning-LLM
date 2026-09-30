SELECT
    s.device,
    s.customer_id,
    s.browser,
    we.event_type,
    we.duration_sec
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE we.event_type IN ('search', 'purchase', 'add_to_cart')
ORDER BY customer_id, event_type;
