SELECT
    s.device,
    s.browser,
    we.page,
    we.event_type,
    we.event_time
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id;
