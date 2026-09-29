SELECT
    s.device,
    s.browser,
    s.country,
    we.event_type
FROM sessions s
LEFT OUTER JOIN web_events we ON we.session_id = s.id
WHERE we.event_type != 'page_view' AND s.device IN ('tablet', 'mobile', 'desktop')
