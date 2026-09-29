SELECT
    s.customer_id,
    s.browser,
    s.started_at,
    we.session_id
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
