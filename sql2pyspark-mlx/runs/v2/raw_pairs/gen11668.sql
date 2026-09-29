SELECT
    s.browser,
    s.device,
    s.customer_id,
    we.page
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
ORDER BY device
