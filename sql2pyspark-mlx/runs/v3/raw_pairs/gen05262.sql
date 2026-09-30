SELECT
    s.started_at,
    s.customer_id,
    we.event_time,
    we.duration_sec,
    we.session_id,
    DATE_TRUNC('month', we.event_time) AS event_time_period
FROM sessions s
JOIN web_events we ON we.session_id = s.id
WHERE we.page IN ('/home', '/product', '/cart') OR s.country IN ('DE', 'TH')
ORDER BY customer_id DESC;
