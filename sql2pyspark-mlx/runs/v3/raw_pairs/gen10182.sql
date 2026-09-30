SELECT
    s.browser,
    s.customer_id,
    s.device,
    we.event_time,
    we.page,
    we.session_id
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE we.event_type = 'page_view' AND we.event_time >= '2024-12-01'
ORDER BY we.page DESC;
